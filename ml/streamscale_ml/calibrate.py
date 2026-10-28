"""Fit simulator parameters to M7's committed measurements (fixed and lag-KEDA medians).

The fit minimises relative error in peak lag, replica-seconds and scaling response. The gate
accepts a simulated peak lag or replica-seconds value within 20% of the M7 median or inside
the measured [min, max] across repetitions. Results go to ``ml/calibration.json``. CPU-HPA is
reported for comparison only: its metrics pipeline is approximated and not gated.
"""

import argparse
import csv
import json
import statistics
from dataclasses import replace
from pathlib import Path

from streamscale_workload.shapes import sale_starts, schedule

from streamscale_ml.sim import Cluster, CpuHpaLike, FixedController, KedaLike
from streamscale_ml.sim import run as run_simulation

ROOT = Path(__file__).resolve().parents[2]
M7 = ROOT / "docs/results/current"
CALIBRATION = ROOT / "ml/calibration.json"
GATE = ("fixed", "lag-keda")
SCENARIOS = ("flash-sale", "overload-recovery")
METRICS = {
    "peak_lag": "maximum_sampled_lag",
    "replica_seconds": "replica_seconds",
    "p95_latency": "p95_end_to_end_seconds",
    "scaling_response": "scaling_response_seconds",
}
FITTED = ("peak_lag", "replica_seconds", "scaling_response")
GATED = ("peak_lag", "replica_seconds")
TOLERANCE = 0.20  # relative, for peak lag and replica-seconds


def measured(directory: Path = M7):
    rows = list(csv.DictReader((directory / "summary.csv").open()))
    frozen = json.loads((directory / "frozen-protocol.json").read_text())
    targets = {}
    for scenario in ("steady", *SCENARIOS):
        for mode in ("fixed", "cpu-hpa", "lag-keda"):
            cell = [row for row in rows if row["scenario"] == scenario and row["mode"] == mode]
            targets[(scenario, mode)] = {}
            for key, column in METRICS.items():
                values = [float(row[column]) for row in cell if row[column] != ""]
                targets[(scenario, mode)][key] = statistics.median(values) if values else None
                targets[(scenario, mode)][key + "_range"] = (
                    (min(values), max(values)) if values else None
                )
    return targets, frozen["rates"]


def controller(mode):
    return {"fixed": FixedController, "lag-keda": KedaLike, "cpu-hpa": CpuHpaLike}[mode]()


def simulate(cluster, scenario, mode, rates):
    phases = schedule(scenario, "train:0", rates["steady"], rates["surge"])
    return run_simulation(phases, controller(mode), cluster, sale_starts(phases, rates["steady"]))


def loss(cluster, targets, rates, modes):
    total = 0.0
    for scenario in SCENARIOS:
        for mode in modes:
            result = simulate(cluster, scenario, mode, rates)
            for key in FITTED:
                expected = targets[(scenario, mode)][key]
                if expected is None:
                    continue
                simulated = getattr(result, key)
                if simulated is None:
                    total += 1.0
                    continue
                total += ((simulated - expected) / expected) ** 2
    return total


def fit(targets, rates, top=1):
    """Coordinate search: capacity from fixed runs, then KEDA timing parameters.

    Returns the ``top`` best candidates against M7, best first.
    """
    cluster = Cluster()
    best = min(
        (replace(cluster, capacity=float(value)) for value in range(115, 151)),
        key=lambda item: loss(item, targets, rates, ("fixed",)),
    )
    candidates = [
        replace(
            best,
            capacity=best.capacity + shift,
            ready_delay=delay,
            hpa_phase=float(phase),
            efficiency_3=efficiency,
            rebalance_pause=pause,
            probe_lag=probe,
        )
        for shift in (-2, 0, 2)
        for delay in (2.0, 3.0, 4.0, 5.0, 6.0)
        for phase in range(0, 15)
        for efficiency in (0.85, 0.9, 0.95, 1.0)
        for pause in (0.5, 1.0)
        for probe in (0.0, 1.0, 2.0, 3.0)
    ]
    return sorted(candidates, key=lambda item: loss(item, targets, rates, GATE))[:top]


def compare(cluster, targets, rates):
    report = []
    for scenario in ("steady", *SCENARIOS):
        for mode in ("fixed", "cpu-hpa", "lag-keda"):
            result = simulate(cluster, scenario, mode, rates)
            expected = targets[(scenario, mode)]
            row = {"scenario": scenario, "mode": mode, "gated": mode in GATE}
            for key in METRICS:
                simulated, value = getattr(result, key), expected[key]
                row[key] = {
                    "simulated": None if simulated is None else round(simulated, 2),
                    "measured": None if value is None else round(value, 2),
                    "measured_range": expected[key + "_range"],
                }
            report.append(row)
    return report


def check(report):
    failures = []
    for row in report:
        if not row["gated"] or row["scenario"] == "steady":
            continue
        for key in GATED:
            simulated, expected = row[key]["simulated"], row[key]["measured"]
            low, high = row[key]["measured_range"]
            inside = low <= simulated <= high
            if not inside and abs(simulated - expected) > TOLERANCE * expected:
                failures.append(f"{row['scenario']}/{row['mode']} {key}: {simulated} vs {expected}")
    return failures


CLUSTER_TOLERANCE = 0.30  # one repetition per cell, so looser than the M7 gate
CLUSTER_MODES = ("fixed", "cpu-hpa", "lag-keda", "ml-reactive")


def cluster_runs(campaign: Path):
    """Measured runs of a calibration campaign (train: shapes only) as fit targets."""
    state = json.loads((campaign / "campaign.json").read_text())
    if state.get("purpose") != "calibration":
        raise ValueError("Only calibration campaigns (train: shape seeds) may fit the simulator")
    frozen = json.loads((campaign / "frozen-protocol.json").read_text())
    if frozen.get("shape_seed_namespace") != "train":
        raise ValueError("Calibration campaign used non-training shape seeds")
    runs = []
    for row in state["rows"]:
        if row["phase"] != "measured" or row["mode"] not in CLUSTER_MODES:
            continue
        configuration = json.loads(
            (ROOT / row["artifact_directory"] / "configuration.json").read_text()
        )
        runs.append(
            {
                "mode": row["mode"],
                "scenario": row["scenario"],
                "schedule": configuration["schedule"],
                "sale_starts": configuration.get("sale_starts", []),
                "peak_lag": float(row["maximum_sampled_lag"]),
                "replica_seconds": float(row["replica_seconds"]),
                "p95_latency": float(row["p95_end_to_end_seconds"]),
            }
        )
    return runs, float(frozen["pilot_capacity_per_second"])


def simulate_run(cluster, run, pilot):
    from streamscale_ml.policies import controller as policy_controller

    simulated = (
        policy_controller(run["mode"], pilot)
        if run["mode"].startswith("ml-")
        else controller(run["mode"])
    )
    return run_simulation(run["schedule"], simulated, cluster, run["sale_starts"])


def cluster_loss(cluster, runs, pilot):
    total = 0.0
    for run in runs:
        result = simulate_run(cluster, run, pilot)
        for key in GATED:
            total += ((getattr(result, key) - run[key]) / run[key]) ** 2
    return total


def refine(candidates, runs, pilot, m7_pilot):
    """Refit timing and 3-consumer efficiency to this cluster's calibration runs.

    Pod start, readiness-probe lag and HPA phase depend on the current host, so they are
    refitted here; capacity is scaled to the calibration campaign's own pilot.
    """
    best, best_loss = None, None
    for candidate in candidates:
        for efficiency in (0.7, 0.8, 0.9, 1.0):
            for delay in (2.0, 4.0, 6.0, 8.0, 10.0):
                for probe in (0.0, 1.5, 3.0):
                    for phase in (2.0, 6.0, 10.0, 14.0):
                        trial = replace(
                            candidate,
                            efficiency_3=efficiency,
                            ready_delay=delay,
                            probe_lag=probe,
                            hpa_phase=phase,
                        )
                        local = replace(trial, capacity=trial.capacity * pilot / m7_pilot)
                        value = cluster_loss(local, runs, pilot)
                        if best_loss is None or value < best_loss:
                            best, best_loss = trial, value
    return best


def compare_cluster(cluster, runs, pilot, m7_pilot):
    local = replace(cluster, capacity=cluster.capacity * pilot / m7_pilot)
    rows, failures = [], []
    for run in runs:
        result = simulate_run(local, run, pilot)
        row = {"scenario": run["scenario"], "mode": run["mode"]}
        for key in (*GATED, "p95_latency"):
            row[key] = {"simulated": round(getattr(result, key), 2), "measured": run[key]}
        for key in GATED:
            if abs(getattr(result, key) - run[key]) > CLUSTER_TOLERANCE * run[key]:
                failures.append(
                    f"cluster {run['scenario']}/{run['mode']} {key}: "
                    f"{row[key]['simulated']} vs {run[key]}"
                )
        rows.append(row)
    return rows, failures


def load(path: Path = CALIBRATION) -> Cluster:
    if not path.exists():
        return Cluster()
    return Cluster(**json.loads(path.read_text())["cluster"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=CALIBRATION)
    parser.add_argument(
        "--campaign",
        type=Path,
        help="Calibration campaign (train: shapes) to refine 3-consumer efficiency",
    )
    args = parser.parse_args()
    targets, rates = measured()
    m7_pilot = float(
        json.loads((M7 / "frozen-protocol.json").read_text())["pilot_capacity_per_second"]
    )
    candidates = fit(targets, rates, top=3 if args.campaign else 1)
    cluster = candidates[0]
    cluster_report, cluster_failures, source = [], [], None
    if args.campaign:
        runs, pilot = cluster_runs(args.campaign)
        if not runs:
            raise SystemExit("The calibration campaign has no usable measured runs")
        cluster = refine(candidates, runs, pilot, m7_pilot)
        cluster_report, cluster_failures = compare_cluster(cluster, runs, pilot, m7_pilot)
        source = str(args.campaign.resolve().relative_to(ROOT))
    report = compare(cluster, targets, rates)
    failures = check(report) + cluster_failures
    payload = {
        "source": "docs/results/current (M7 v2 medians)",
        "cluster_source": source,
        "rates": rates,
        "cluster": cluster.to_dict(),
        "tolerance": TOLERANCE,
        "cluster_tolerance": CLUSTER_TOLERANCE,
        "comparison": report,
        "cluster_comparison": cluster_report,
        "failures": failures,
    }
    args.output.write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps({"cluster": cluster.to_dict(), "failures": failures}, indent=2))
    for row in report:
        cells = "  ".join(
            f"{key}={row[key]['simulated']}/{row[key]['measured']}" for key in METRICS
        )
        print(f"{row['scenario']:18} {row['mode']:9} {cells}")
    for row in cluster_report:
        cells = "  ".join(
            f"{key}={row[key]['simulated']}/{row[key]['measured']}"
            for key in (*GATED, "p95_latency")
        )
        print(f"cluster {row['scenario']:14} {row['mode']:12} {cells}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

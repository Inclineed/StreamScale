"""Choose the forecaster quantile on training seeds, then train and export the final model.

The quantile sets the policy's risk level: higher quantiles add consumers on a smaller
chance of a sale. Candidates are scored in closed-loop simulation on a training-namespace
tuning range that neither model fitting nor evaluation uses. The objective follows the M16
requirement: first maximise the share of runs whose p95 latency meets the target, then use
the fewest replica-seconds among candidates within ``TIE`` of the best share.
"""

import argparse
import json
import statistics
import tempfile
from pathlib import Path

from streamscale_workload.shapes import SHAPES, require_namespace, sale_starts, schedule

from streamscale.autoscale.forecast import load as load_model
from streamscale.autoscale.planner import PlannerConfig
from streamscale_ml.calibrate import load
from streamscale_ml.policies import controller, pilot
from streamscale_ml.sim import run
from streamscale_ml.train import MODEL, REPORT, print_report, train

ROOT = Path(__file__).resolve().parents[2]
TUNING = ROOT / "ml/tuning.json"
QUANTILES = (0.5, 0.6, 0.7, 0.8, 0.9)
TUNING_SEEDS = range(200_000, 200_020)
TIE = 0.02


def score(model, seeds, cluster, capacity, rates):
    target = PlannerConfig().target_delay_seconds
    met, replica_seconds, per_shape = [], [], {}
    for shape in SHAPES:
        shape_met = []
        for seed in seeds:
            phases = schedule(shape, seed, rates["steady"], rates["surge"])
            result = run(
                phases,
                controller("ml-predictive", capacity, model),
                cluster,
                sale_starts(phases, rates["steady"]),
            )
            shape_met.append(result.p95_latency <= target)
            replica_seconds.append(result.replica_seconds)
        met.extend(shape_met)
        per_shape[shape] = round(sum(shape_met) / len(shape_met), 3)
    return {
        "target_met": round(sum(met) / len(met), 4),
        "median_replica_seconds": round(statistics.median(replica_seconds), 1),
        "per_shape_target_met": per_shape,
    }


def choose(rows):
    best = max(row["target_met"] for row in rows)
    tied = [row for row in rows if row["target_met"] >= best - TIE]
    return min(tied, key=lambda row: (row["median_replica_seconds"], row["quantile"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-traces", type=int, default=300)
    parser.add_argument("--traces", type=int, default=700)
    parser.add_argument("--holdout", type=int, default=140)
    parser.add_argument("--output", type=Path, default=MODEL)
    args = parser.parse_args()
    rates, capacity = pilot()
    cluster = load()
    seeds = [require_namespace(f"train:{index}", "train") for index in TUNING_SEEDS]
    rows = []
    with tempfile.TemporaryDirectory() as scratch:
        for quantile in QUANTILES:
            path = Path(scratch) / f"q{quantile}.json"
            train(args.candidate_traces, 20, quantile, path)
            row = {"quantile": quantile, **score(load_model(path), seeds, cluster, capacity, rates)}
            rows.append(row)
            print(json.dumps(row))
    chosen = choose(rows)
    tuning = {
        "objective": "max share of runs with p95 <= target, then min median replica-seconds",
        "tie_tolerance": TIE,
        "tuning_seeds": f"train:{TUNING_SEEDS.start}..train:{TUNING_SEEDS.stop - 1}",
        "candidates": rows,
        "chosen_quantile": chosen["quantile"],
    }
    TUNING.write_text(json.dumps(tuning, indent=2) + "\n")
    report = train(
        args.traces,
        args.holdout,
        chosen["quantile"],
        args.output,
        {"tuning": {key: tuning[key] for key in ("objective", "tuning_seeds", "chosen_quantile")}},
    )
    REPORT.write_text(json.dumps({**report, "tuning": tuning}, indent=2) + "\n")
    print_report(report)


if __name__ == "__main__":
    main()

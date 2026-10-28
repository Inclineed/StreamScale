"""Generate standalone scientific plots and a report from a complete audited matrix."""

import argparse
import csv
import hashlib
import json
import os
import shutil
import statistics
from pathlib import Path

from experiments.evidence import MODES, ROOT, SCENARIOS, audit, read, seconds

COLORS = {
    "fixed": "#3266a8",
    "cpu-hpa": "#cc7831",
    "lag-keda": "#2b896a",
    "ml-reactive": "#8a6bb8",
    "ml-predictive": "#c2416b",
}
LABELS = {
    "fixed": "Fixed",
    "cpu-hpa": "CPU-HPA",
    "lag-keda": "Lag-KEDA",
    "ml-reactive": "ML-reactive",
    "ml-predictive": "ML-predictive",
}
# Protocol v3 adds per-sale readiness/response and controller churn.
V3_METRICS = ("mean_ready_at_sale_start", "mean_sale_response_seconds", "scale_changes")
METRICS = (
    "throughput_window_per_second",
    "p95_end_to_end_seconds",
    "maximum_sampled_lag",
    "recovery_seconds",
    "scaling_response_seconds",
    "replica_seconds",
    "average_cpu_cores",
    "peak_memory_mib",
    "lag_sample_coverage",
    "max_ready_replicas",
)


def aggregate(rows, modes=MODES, scenarios=SCENARIOS, metrics=METRICS, repetitions=3):
    results = []
    for scenario in scenarios:
        for mode in modes:
            selected = [row for row in rows if row["scenario"] == scenario and row["mode"] == mode]
            expected = set(range(1, repetitions + 1))
            if len(selected) != repetitions or {row["repetition"] for row in selected} != expected:
                raise RuntimeError(
                    "Each comparison requires three distinct measured repetitions"
                    if repetitions == 3
                    else f"Each comparison requires {repetitions} distinct measured repetitions"
                )
            for metric in metrics:
                values = [row[metric] for row in selected if row.get(metric) is not None]
                results.append(
                    {
                        "scenario": scenario,
                        "mode": mode,
                        "metric": metric,
                        "observed": len(values),
                        "missing": repetitions - len(values),
                        "median": statistics.median(values) if values else None,
                        "minimum": min(values) if values else None,
                        "maximum": max(values) if values else None,
                    }
                )
    return results


def csv_output(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def value_table(aggregates, metric):
    return {(row["scenario"], row["mode"]): row for row in aggregates if row["metric"] == metric}


def display(row, digits=2):
    if row["median"] is None:
        return "unobserved"
    return f"{row['median']:.{digits}f} [{row['minimum']:.{digits}f}, {row['maximum']:.{digits}f}]"


def report(campaign, state, frozen, rows, aggregates, verification):
    throughput = value_table(aggregates, "throughput_window_per_second")
    latency = value_table(aggregates, "p95_end_to_end_seconds")
    lag = value_table(aggregates, "maximum_sampled_lag")
    replicas = value_table(aggregates, "replica_seconds")
    recovery = value_table(aggregates, "recovery_seconds")
    resources = verification["consumer_resources"][0]
    lines = [
        "# StreamScale measured results",
        "",
        f"Campaign `{campaign.name}` completed {state['ended_at']}. All **27 measured runs** "
        "passed acknowledgement/SQL/DLQ reconciliation before and after a controlled restart.",
        "",
        f"Current-campaign excluded attempts: **{len(state['failures'])}**. The independent audit "
        f"verified {verification['raw_samples']:,} raw samples, SQL metrics, CSV values, fresh "
        "consumer groups and unchanged actual consumer image/resource settings.",
        "",
        f"Protocol **v{frozen['protocol_version']}**: pilot capacity "
        f"{frozen['pilot_capacity_per_second']:.2f} accepted events/second; frozen steady rate "
        f"**{frozen['rates']['steady']}**, surge rate "
        f"**{frozen['rates']['surge']}** events/second. "
        "Each scheduled measurement window is 60 seconds, after a separate 30-second cache warmup. "
        "Each execution uses a fresh run UUID and consumer group. "
        "Measured group startup is included.",
        "",
        "Values below are **median [minimum, maximum] across three repetitions**. "
        "Latency is the median of three independently computed run p95 values, not a pooled "
        "percentile or an average of pod percentiles. No confidence interval or statistical "
        "significance is claimed.",
        "",
        "| Scenario | Mode | Window throughput (facts/s) | Run p95 latency (s) "
        "| Maximum sampled lag |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for scenario in SCENARIOS:
        for mode in MODES:
            key = (scenario, mode)
            lines.append(
                f"| {scenario} | {LABELS[mode]} | {display(throughput[key])} | "
                f"{display(latency[key])} | {display(lag[key], 0)} |"
            )
    lines += [
        "",
        "![Primary outcomes](primary-outcomes.svg)",
        "",
        "Dots are individual repetitions; diamonds are medians and whiskers span "
        "the observed range.",
        "",
    ]
    for scenario in SCENARIOS:
        if scenario == "steady":
            values = ", ".join(
                f"{LABELS[mode]} {latency[(scenario, mode)]['median']:.3f}s" for mode in MODES
            )
            ready = max(row["max_ready_replicas"] for row in rows if row["scenario"] == scenario)
            lines.append(
                f"Steady median run p95 values were **{values}**; the maximum observed "
                f"ready-consumer count was {ready}. Their small differences are "
                "descriptive observations from three repetitions."
            )
            lines.append("")
            continue
        best = min(MODES, key=lambda mode: latency[(scenario, mode)]["median"])
        lines.append(
            f"For **{scenario}**, {LABELS[best]} had the lowest observed median run p95 "
            f"({latency[(scenario, best)]['median']:.2f} seconds). Its median ready "
            f"replica-seconds were {replicas[(scenario, best)]['median']:.2f}, compared with "
            f"{replicas[(scenario, 'fixed')]['median']:.2f} for fixed. The paired table shows "
            "throughput and lag; these results describe this local campaign only."
        )
        lines.append("")
    lines += [
        "| Scenario | Mode | Ready replica-seconds | Recovery after overload (s) "
        "| Observed recoveries |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for scenario in SCENARIOS:
        for mode in MODES:
            key = (scenario, mode)
            observed = recovery[key]["observed"] if scenario == "overload-recovery" else "n/a"
            recovered = display(recovery[key]) if scenario == "overload-recovery" else "n/a"
            lines.append(
                f"| {scenario} | {LABELS[mode]} | {display(replicas[key])} "
                f"| {recovered} | {observed} |"
            )
    lines += [
        "",
        "![Resource outcomes](resource-outcomes.svg)",
        "",
        "![Lag and ready replica trajectories](trajectories.svg)",
        "",
        "Trajectory lines show each repetition separately, aligned to publication start; "
        "shading indicates the scheduled surge and the vertical line the end of the base window. "
        "Lines continue through confirmed drain. Ready pods differ from desired replicas "
        "and Kafka members.",
        "",
        "The primary throughput counts unique facts ingested in the half-open 60-second window. "
        "It can be lower during a burst even when every event eventually arrives. "
        "`summary.csv` also reports processing-span and drain-inclusive throughput. "
        "Latency uses every unique accepted event, including the drained tail. Maximum lag is "
        "the maximum sampled sum of three broker-offset partitions. Recovery ends after five "
        "distinct fresh zero-lag observations, including their confirmation delay. "
        "Missing recovery "
        "or scaling response remains missing; it is never filled with zero.",
        "",
        "The shared correctness profile delays 5% of the base event count (rounded down "
        "and selected from valid events) by ten seconds, alongside invalid/duplicate fixtures. "
        "These late attempts extend actual publication to about 70 seconds; `publish_seconds` "
        "records the observed duration. The primary window remains the frozen base 60 seconds "
        "for every mode. Delayed tail arrivals and the five-fresh-zero criterion constrain "
        "measured recovery, so similar recovery times can coexist with very different burst "
        "p95 latency. Input-rate labels describe the base logical schedule; fault attempts "
        "alter the observed attempt rate. The complete delayed tail is reconciled and included "
        "in p95.",
        "",
        "Resource integrals cover publication through confirmed drain, before the controlled "
        "restart. They use actual observation times with left-step interpolation. Kubernetes CPU "
        "metrics are delayed averages, sampled every five seconds; missing leading measurements "
        "are disclosed by coverage columns. Maximum memory is a sampled sum across available "
        "consumer pods. These resource values are approximations, not billing measurements.",
        "",
        f"Image `{state['image']['image']}`; build digest `{state['image']['image_id']}`; "
        f"Git `{state['image']['git_commit']}`; dirty build: `{state['image']['git_dirty']}`. "
        "The platform manifest IDs observed in pods are separately preserved in `audit.json`.",
        "",
        f"Consumer requests: {resources['requests']['cpu']} CPU / "
        f"{resources['requests']['memory']}; limits: {resources['limits']['cpu']} CPU / "
        f"{resources['limits']['memory']} per replica. Fixed uses "
        "one replica; CPU-HPA targets 60% of requested CPU; KEDA targets 100 Kafka records of "
        "lag per replica. Autoscaled modes are bounded at 1–3, with three input partitions. "
        "Controllers are alternative, never simultaneously attached to the consumer.",
        "",
        "The same logical scenario manifest must match across all nine corresponding cells. "
        "Seed 4242 randomizes cell order. All rates/windows were frozen before measurement; "
        "no measured run was tuned individually. Run IDs, logical hashes, correctness outcomes "
        "and exact configuration appear in the companion CSV/protocol/provenance files.",
        "",
        "The single kind node shares host CPU with Kafka, PostgreSQL, monitoring and the observer. "
        "The host, retained database growth, cache carryover, short windows, metrics/controller "
        "cadence and fresh-group startup may affect results. Randomized order reduces but does "
        "not eliminate carryover. Three repetitions give a small descriptive sample. Sampled "
        "peaks can miss intermediate maxima. This v2 60-second calibration is not directly "
        "comparable to the original 120-second fixed-rate proposal. The controlled restart "
        "occurs after measurement; crash-during-commit replay was separately exercised in M3/M4. "
        "The pipeline remains at-least-once, and DLQ redelivery can create extra copies. "
        "No production, multi-node or exactly-once improvement is claimed.",
        "",
        "Reproduce with `./streamscale.sh all --no-ui`; reuse validates retained raw evidence. "
        "For new measurements use `./streamscale.sh experiments --fresh`, then "
        "`./streamscale.sh analysis`. Raw artifacts are ignored locally and identified by "
        "`artifacts/experiments/latest.path`. See [the protocol](../../experiments.md) and "
        "[presentation walkthrough](../../presentation.md).",
        "",
    ]
    return "\n".join(lines)


def schedule_for(frozen, scenario):
    """Repetition-1 schedule for shading; v3 shapes vary by repetition."""
    entry = frozen["scenarios"][scenario]
    if "repetitions" in entry:
        return entry["repetitions"]["1"]["schedule"]
    return entry["schedule"]


def report_v3(campaign, state, frozen, rows, aggregates, verification, scratch=False, subset=None):
    modes, scenarios = subset or matrix(state)
    table = {metric: value_table(aggregates, metric) for metric in (*METRICS, *V3_METRICS)}
    model = frozen["model"]
    settings = frozen["autoscaler"]
    repetitions = max(cell["repetition"] for cell in state["plan"])
    count = len(state["plan"])
    lines = [
        "# StreamScale M16 measured results: ML predictive autoscaling",
        "",
    ]
    if scratch:
        lines += [
            "> **Scratch subset, not evidence.** This report checks the pipeline end to end; "
            "it is not the frozen campaign and must not be cited as a result.",
            "",
        ]
    lines += [
        f"Campaign `{campaign.name}` completed {state['ended_at']}. All **{count} measured "
        "runs** passed acknowledgement/SQL/DLQ reconciliation before and after a controlled "
        f"restart. Excluded attempts in this campaign: **{len(state['failures'])}**. The "
        f"independent audit verified {verification['raw_samples']:,} raw samples.",
        "",
        f"Protocol **v{frozen['protocol_version']}**: pilot capacity "
        f"{frozen['pilot_capacity_per_second']:.2f} accepted events/s; steady "
        f"**{frozen['rates']['steady']}**/s, surge **{frozen['rates']['surge']}**/s. "
        f"{len(modes)} modes × {len(scenarios)} scenarios × {repetitions} repetitions in "
        f"seed-{frozen['order_seed']} randomized order. Ramp and repeated-sales shapes vary "
        "by repetition from reserved `eval:*` seeds; every mode in a repetition sees the same "
        "logical sequence.",
        "",
        "**Modes.** ML-predictive forecasts the peak arrival rate in four windows over the "
        f"next 20 s (quantile {model['quantile']} gradient-boosted trees, sha256 "
        f"`{model['sha256'][:12]}…`, trained on `{model['training_seeds']}` in a simulator "
        "calibrated against M7) and adapts its bias, per-consumer capacity and pod-start "
        "delay online. ML-reactive is the ablation: the same one-second planner and actuation, "
        "assuming the current arrival rate continues and learning nothing. Both choose the "
        f"fewest consumers whose predicted queueing delay stays within "
        f"{settings['target_delay_seconds']} s, using frozen capacity "
        f"{settings['capacity']:.2f}/s per consumer. Neither can read the run schedule. "
        "Fixed, CPU-HPA and Lag-KEDA keep their M7 settings.",
        "",
        "Values are **median [minimum, maximum] across repetitions**. No confidence interval "
        "or statistical significance is claimed.",
        "",
        "| Scenario | Mode | Run p95 latency (s) | Max sampled lag | Ready replica-s "
        "| Ready at sale start | Sale response (s) | Scale changes |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for scenario in scenarios:
        for mode in modes:
            key = (scenario, mode)
            lines.append(
                f"| {scenario} | {LABELS[mode]} | {display(table['p95_end_to_end_seconds'][key])}"
                f" | {display(table['maximum_sampled_lag'][key], 0)}"
                f" | {display(table['replica_seconds'][key], 0)}"
                f" | {display(table['mean_ready_at_sale_start'][key])}"
                f" | {display(table['mean_sale_response_seconds'][key], 1)}"
                f" | {display(table['scale_changes'][key], 0)} |"
            )
    lines += ["", "![Primary outcomes](primary-outcomes.svg)", ""]
    latency = table["p95_end_to_end_seconds"]
    replicas = table["replica_seconds"]
    for scenario in scenarios:
        ordered = sorted(modes, key=lambda mode: latency[(scenario, mode)]["median"])
        best = ordered[0]
        text = (
            f"For **{scenario}**, {LABELS[best]} had the lowest observed median run p95 "
            f"({latency[(scenario, best)]['median']:.2f} s, "
            f"{replicas[(scenario, best)]['median']:.0f} replica-s)."
        )
        if {"ml-predictive", "ml-reactive", "lag-keda"} <= set(modes):
            predictive = (scenario, "ml-predictive")
            text += (
                f" ML-predictive: {latency[predictive]['median']:.2f} s and "
                f"{replicas[predictive]['median']:.0f} replica-s; ML-reactive: "
                f"{latency[(scenario, 'ml-reactive')]['median']:.2f} s and "
                f"{replicas[(scenario, 'ml-reactive')]['median']:.0f} replica-s; Lag-KEDA: "
                f"{latency[(scenario, 'lag-keda')]['median']:.2f} s and "
                f"{replicas[(scenario, 'lag-keda')]['median']:.0f} replica-s."
            )
        lines += [text, ""]
    lines += [
        "![Resource outcomes](resource-outcomes.svg)",
        "",
        "![Lag and ready replica trajectories](trajectories.svg)",
        "",
        "## How to read the ML comparison",
        "",
        "- ML-reactive versus Lag-KEDA mostly measures control-loop speed: a one-second loop "
        "that sets replicas directly, against KEDA's 2 s poll plus the HPA's 15 s sync.",
        "- ML-predictive versus ML-reactive isolates the forecast and online learning; the "
        "forecast can only add consumers earlier, never remove them sooner.",
        "- Ready at sale start counts consumers already Ready when each sale began, averaged "
        "over a run's sales. Sale response is the time from a sale's start to the next rise "
        "in ready consumers (missing when none was needed or observed).",
        "",
        "## Limitations",
        "",
        "- Evaluation shapes come from the same generator and parameter ranges as training, "
        "under different seeds. Real traffic is not claimed to follow them.",
        "- The forecaster was trained in a simulator calibrated to M7 medians (±20% or within "
        "the measured range); simulator error carries into the model.",
        "- One shared kind node, three repetitions and short windows; carryover, controller "
        "cadence and fresh-group startup may affect results. Descriptive only.",
        "- At-least-once delivery; no production, multi-node or exactly-once improvement is "
        "claimed. Resource integrals are approximations, not billing.",
        "",
        f"Image `{state['image']['image']}`; Git `{state['image']['git_commit']}`; dirty build: "
        f"`{state['image']['git_dirty']}`. Reproduce with `./streamscale.sh experiments-v3 "
        "--fresh`, then `./streamscale.sh analysis-v3`. See [M16 evidence](../../m16-evidence.md).",
        "",
    ]
    return "\n".join(lines)


def matrix(state):
    from experiments.evidence import matrix as campaign_matrix

    return campaign_matrix(state)


def plots(rows, frozen, destination, modes=MODES, scenarios=SCENARIOS, title=None):
    os.environ.setdefault("MPLCONFIGDIR", str(ROOT / "artifacts/matplotlib-cache"))
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    plt.rcParams.update(
        {
            "font.size": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "svg.fonttype": "none",
            "svg.hashsalt": "streamscale-v2",
        }
    )
    figures = []

    width = 11.7 if len(scenarios) <= 3 else 3.6 * len(scenarios)
    repetitions = max((row["repetition"] for row in rows), default=3)

    def outcomes(name, metrics):
        figure, axes = plt.subplots(
            len(metrics), len(scenarios), figsize=(width, 8.3), layout="constrained", squeeze=False
        )
        for column, scenario in enumerate(scenarios):
            for index, (metric, label) in enumerate(metrics):
                axis = axes[index, column]
                for position, mode in enumerate(modes):
                    points = [
                        row[metric]
                        for row in sorted(rows, key=lambda row: row["repetition"])
                        if row["scenario"] == scenario
                        and row["mode"] == mode
                        and row.get(metric) is not None
                    ]
                    if not points:
                        continue
                    median = statistics.median(points)
                    axis.scatter(
                        [position + (i - (len(points) - 1) / 2) * 0.13 for i in range(len(points))],
                        points,
                        color=COLORS[mode],
                        s=22,
                        alpha=0.7,
                    )
                    axis.errorbar(
                        position,
                        median,
                        yerr=[[median - min(points)], [max(points) - median]],
                        fmt="D",
                        color=COLORS[mode],
                        capsize=5,
                        markersize=5,
                    )
                axis.set_xticks(
                    range(len(modes)),
                    [LABELS[mode] for mode in modes],
                    rotation=30 if len(modes) > 3 else 0,
                    fontsize=8 if len(modes) > 3 else None,
                )
                axis.set_ylabel(label)
                values = [
                    row[metric]
                    for row in rows
                    if row["scenario"] == scenario and row.get(metric) is not None
                ]
                maximum = max(values, default=0)
                axis.set_ylim(0, maximum * 1.12 if maximum > 0 else 1)
                axis.grid(axis="y", alpha=0.2)
                if index == 0:
                    axis.set_title(scenario.replace("-", " ").title())
        figure.suptitle(
            "StreamScale · " + name.replace("-", " ") + f" · n={repetitions} per cell", fontsize=14
        )
        for suffix in ("svg", "png"):
            figure.savefig(
                destination / f"{name}.{suffix}",
                dpi=160,
                metadata={"Date": None} if suffix == "svg" else None,
            )
        figures.append(figure)

    outcomes(
        "primary-outcomes",
        [
            ("throughput_window_per_second", "Window throughput (unique facts/s)"),
            ("p95_end_to_end_seconds", "Per-run p95 end-to-end latency (s)"),
            ("maximum_sampled_lag", "Maximum sampled lag (records)"),
        ],
    )
    outcomes(
        "resource-outcomes",
        [
            ("replica_seconds", "Ready replica-seconds (including drain)"),
            ("average_cpu_cores", "Average observed consumer CPU (cores)"),
            ("peak_memory_mib", "Peak sampled consumer memory (MiB)"),
        ],
    )
    figure, axes = plt.subplots(
        len(scenarios),
        2,
        figsize=(11.7, max(8.3, 2.8 * len(scenarios))),
        layout="constrained",
        squeeze=False,
    )
    paired = "repetitions" in next(iter(frozen["scenarios"].values()))
    for index, scenario in enumerate(scenarios):
        for row in rows:
            if row["scenario"] != scenario:
                continue
            if paired and row["repetition"] != 1:
                # v3 shapes differ by repetition; trajectories overlay the first one only.
                continue
            source = ROOT / row["artifact_directory"] / "samples.jsonl"
            samples = [json.loads(line) for line in source.read_text().splitlines()]
            start = seconds(row["measurement_start"])
            samples = [sample for sample in samples if sample["time"] >= start]
            lag = [sample for sample in samples if sample["lag"] is not None]
            label = LABELS[row["mode"]] if row["repetition"] == 1 and index == 0 else None
            axes[index, 0].plot(
                [sample["time"] - start for sample in lag],
                [sum(sample["lag"].values()) for sample in lag],
                color=COLORS[row["mode"]],
                alpha=0.65,
                linewidth=1,
                label=label,
            )
            axes[index, 1].step(
                [sample["time"] - start for sample in samples],
                [sample["ready"] for sample in samples],
                where="post",
                color=COLORS[row["mode"]],
                alpha=0.65,
                linewidth=1,
                label=label,
            )
        for column in range(2):
            axis = axes[index, column]
            elapsed = 0
            for duration, rate in schedule_for(frozen, scenario):
                surge = (
                    rate == frozen["rates"]["surge"]
                    if not paired
                    else rate > frozen["rates"]["steady"]
                )
                if surge:
                    axis.axvspan(elapsed, elapsed + duration, color="#9d8096", alpha=0.12)
                elapsed += duration
            axis.axvline(elapsed, color="#555555", linestyle="--", linewidth=0.8)
            axis.set_title(scenario.replace("-", " ").title())
            axis.set_xlabel("Seconds since publication began")
            axis.set_ylabel("Sampled lag (records)" if column == 0 else "Ready consumers")
            axis.set_ylim(bottom=0)
            axis.grid(alpha=0.15)
        axes[index, 1].set_yticks([0, 1, 2, 3])
        axes[index, 1].set_ylim(-0.1, 3.2)
    axes[0, 0].legend(frameon=False, ncols=3, fontsize=8)
    figure.suptitle(
        "StreamScale · measured trajectories · repetition 1 of each shape"
        if paired
        else "StreamScale · every measured trajectory · three repetitions",
        fontsize=14,
    )
    for suffix in ("svg", "png"):
        figure.savefig(
            destination / f"trajectories.{suffix}",
            dpi=160,
            metadata={"Date": None} if suffix == "svg" else None,
        )
    figures.append(figure)
    with PdfPages(
        destination / "presentation.pdf",
        metadata={
            "Title": "StreamScale measured scaling comparison",
            "CreationDate": None,
            "ModDate": None,
            "Author": "StreamScale",
        },
    ) as pdf:
        introduction = plt.figure(figsize=(11.7, 8.3))
        introduction.text(0.08, 0.86, "StreamScale", fontsize=30, weight="bold")
        introduction.text(
            0.08, 0.78, "Kafka → transactional PostgreSQL · scaling experiment", fontsize=17
        )
        introduction.text(
            0.08,
            0.62,
            title
            or (
                "27 measured runs · 3 modes × 3 scenarios × 3 repetitions\n"
                "All acknowledged IDs reconciled before/after restart\n"
                "Same image, logical scenario sequence, resources and partitions\n"
                "30-second warmup + 60-second base window; late fixtures extend the tail\n"
                f"Frozen rates: {frozen['rates']['steady']}/s steady · "
                f"{frozen['rates']['surge']}/s surge"
            ),
            fontsize=14,
            linespacing=1.8,
            va="top",
        )
        introduction.text(
            0.08,
            0.30,
            "Dots = repetitions; diamonds = median; whiskers = observed range\n"
            "SQL p95 includes the drained tail; throughput uses the publishing window\n"
            "One shared kind node, small sample, sampled resources; descriptive results\n"
            "See report.md, summary.csv, frozen-protocol.json and provenance.json",
            fontsize=12,
            linespacing=1.8,
            va="top",
        )
        pdf.savefig(introduction)
        plt.close(introduction)
        for figure in figures:
            pdf.savefig(figure)
            plt.close(figure)


OUTPUTS = {"v2": ROOT / "docs/results/current", "v3": ROOT / "docs/results/m16"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path)
    parser.add_argument("--protocol", choices=sorted(OUTPUTS), default="v2")
    parser.add_argument("--scratch", action="store_true", help="Analyse the latest scratch subset")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    folder = ROOT / "artifacts/experiments" / ("v3" if args.protocol == "v3" else "")
    if args.scratch:
        folder = folder / "scratch"
    campaign = args.campaign or Path((folder / "latest.path").read_text().strip())
    campaign = campaign.resolve()
    state = read(campaign / "campaign.json")
    scratch = bool(state.get("scratch")) or state.get("purpose") == "calibration"
    output = args.output or (
        ROOT / "artifacts/experiments/scratch-report" if scratch else OUTPUTS[args.protocol]
    )
    destination = output.resolve()
    if scratch and destination.is_relative_to((ROOT / "docs/results").resolve()):
        raise RuntimeError("Scratch campaigns are never written to docs/results")
    verification = audit(campaign)
    frozen = read(campaign / "frozen-protocol.json")
    version = frozen["protocol_version"]
    rows = [
        row | {"peak_memory_mib": row["peak_memory_bytes"] / 1024**2}
        for row in state["rows"]
        if row["phase"] == "measured"
    ]
    if version >= 3:
        modes, scenarios = matrix(state)
        modes = tuple(mode for mode in modes if any(cell["mode"] == mode for cell in state["plan"]))
        scenarios = tuple(
            name for name in scenarios if any(cell["scenario"] == name for cell in state["plan"])
        )
        repetitions = max(cell["repetition"] for cell in state["plan"])
        aggregates = aggregate(rows, modes, scenarios, (*METRICS, *V3_METRICS), repetitions)
    else:
        modes, scenarios = MODES, SCENARIOS
        aggregates = aggregate(rows)
    destination.mkdir(parents=True, exist_ok=True)
    for name in ("summary.csv", "comparison.csv", "frozen-protocol.json", "failed-attempts.csv"):
        shutil.copy2(campaign / name, destination / name)
    (destination / "audit.json").write_text(json.dumps(verification, indent=2) + "\n")
    csv_output(destination / "aggregate.csv", aggregates)
    environment = read(campaign / "environment.json")
    provenance = {
        "campaign": campaign.name,
        "started_at": state["started_at"],
        "ended_at": state["ended_at"],
        "image": state["image"],
        "fingerprint": state["fingerprint"],
        "host": environment["docker"],
        "node_versions": [
            {
                key: node["status"]["nodeInfo"][key]
                for key in (
                    "kubeletVersion",
                    "kernelVersion",
                    "osImage",
                    "containerRuntimeVersion",
                    "architecture",
                )
            }
            for node in environment["nodes"]["items"]
        ],
        "source_artifact_directory": str(campaign.relative_to(ROOT))
        if campaign.is_relative_to(ROOT)
        else str(campaign),
        "source_checksums": {
            name: hashlib.sha256((campaign / name).read_bytes()).hexdigest()
            for name in ("campaign.json", "frozen-protocol.json", "summary.csv", "comparison.csv")
        },
    }
    (destination / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    if version >= 3:
        text = report_v3(
            campaign, state, frozen, rows, aggregates, verification, scratch, (modes, scenarios)
        )
        title = (
            f"{len(rows)} measured runs · {len(modes)} modes × {len(scenarios)} scenarios\n"
            "ML-predictive vs its ML-reactive ablation and the M7 controllers\n"
            "All acknowledged IDs reconciled before/after restart\n"
            f"Frozen rates: {frozen['rates']['steady']}/s steady · "
            f"{frozen['rates']['surge']}/s surge"
            + ("\nSCRATCH SUBSET — NOT EVIDENCE" if scratch else "")
        )
    else:
        text = report(campaign, state, frozen, rows, aggregates, verification)
        title = None
    (destination / "report.md").write_text(text)
    plots(rows, frozen, destination, modes, scenarios, title)
    label = "M16" if version >= 3 else "M8"
    print(
        f"{label} report: {destination / 'report.md'}\n"
        f"Presentation: {destination / 'presentation.pdf'}"
    )


if __name__ == "__main__":
    main()

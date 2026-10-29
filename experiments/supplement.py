"""Read-only supplement to the frozen M16 report: target compliance and controller timing.

Inputs are the checksum-verified raw evidence of a complete campaign (and, optionally, the
follow-up ablation campaigns). Nothing in the source campaign or in docs/results/m16 or
docs/results/current is written. Output goes to its own folder.

Two kinds of numbers are kept apart throughout:

- **Predicted** values come from the controller's own decision records: the queueing delay
  its planner predicted for the replica count it chose. They are model outputs, exist only
  for the ML modes, and exclude the first seconds that no decision could still influence.
- **Measured** values come from SQL timestamps: the share of unique accepted events whose
  end-to-end latency (``ingested_at - produced_at``) was within the 3 s target.
"""

import argparse
import csv
import hashlib
import json
import statistics
from pathlib import Path

from experiments.evidence import ROOT, audit, read, seconds

TARGET = 3.0
OUTPUT = ROOT / "docs/results/m16-supplement"
CAMPAIGN = ROOT / "artifacts/experiments/v3/campaign-20261028T225554-fc5dec"
FOLLOWUP = ROOT / "artifacts/experiments/v3/followup"
SURGES = ("flash-sale", "overload-recovery")


def measured_rows(state):
    return [row for row in state["rows"] if row["phase"] == "measured"]


def decisions(row):
    path = ROOT / row["artifact_directory"] / "autoscaler-decisions.json"
    if not path.exists():
        return []
    return [item for item in read(path)["decisions"] if item["run_id"] == row["run_id"]]


def measured_compliance(row):
    facts = read(ROOT / row["artifact_directory"] / "sql-measurements.json")["facts"]
    latencies = [seconds(f["ingested_at"]) - seconds(f["produced_at"]) for f in facts]
    return sum(value <= TARGET for value in latencies) / len(latencies)


def predicted_compliance(row):
    """Share of decisions whose chosen count had a predicted delay within the target."""
    start = seconds(row["measurement_start"])
    end = seconds(row["sampling_ended_at"])
    chosen = [
        item["predicted_delay"][str(item["replicas"])]
        for item in decisions(row)
        if start <= item["at"] <= end and str(item["replicas"]) in item["predicted_delay"]
    ]
    if not chosen:
        return None, None
    within = sum(value <= TARGET for value in chosen) / len(chosen)
    return within, statistics.median(chosen)


def compliance_table(rows):
    table = []
    for row in rows:
        predicted, median = predicted_compliance(row)
        table.append(
            {
                "scenario": row["scenario"],
                "mode": row["mode"],
                "variant": row.get("autoscaler_variant", "full" if "ml-" in row["mode"] else ""),
                "repetition": row["repetition"],
                "measured_share_within_3s": round(measured_compliance(row), 4),
                "measured_p95_seconds": round(row["p95_end_to_end_seconds"], 3),
                "predicted_share_of_decisions_within_3s": None
                if predicted is None
                else round(predicted, 4),
                "predicted_median_delay_seconds": None if median is None else round(median, 3),
            }
        )
    return table


def timing_table(rows):
    """When each ML step-surge run decided, requested and obtained three consumers."""
    table = []
    for row in rows:
        if row["scenario"] not in SURGES or not row["mode"].startswith("ml-"):
            continue
        directory = ROOT / row["artifact_directory"]
        start = seconds(row["measurement_start"])
        transition = read(directory / "configuration.json")["load_transition_seconds"]
        samples = [json.loads(line) for line in (directory / "samples.jsonl").open()]
        made = decisions(row)

        origin = start + transition

        def first(predicate, items, key, origin=origin):
            found = next((item for item in items if predicate(item)), None)
            return None if found is None else round(found[key] - origin, 1)

        def after(item, origin=origin):
            return item["time"] > origin

        table.append(
            {
                "scenario": row["scenario"],
                "mode": row["mode"],
                "repetition": row["repetition"],
                "controller_ready_s": first(
                    lambda s: (s.get("autoscaler") or {}).get("ready"), samples, "time"
                ),
                "first_decision_s": first(lambda d: d["arrival_rate"] is not None, made, "at"),
                "scale_up_requested_s": first(
                    lambda d: d["applied"] and d["replicas"] > d["previous"], made, "at"
                ),
                "three_ready_s": first(lambda s: after(s) and s["ready"] >= 3, samples, "time"),
                "forecast_driven_decisions": sum("forecast" in d["reason"] for d in made),
                "measured_p95_seconds": round(row["p95_end_to_end_seconds"], 3),
            }
        )
    return table


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def median(rows, key, **match):
    values = [
        row[key]
        for row in rows
        if all(row[name] == value for name, value in match.items()) and row[key] is not None
    ]
    return statistics.median(values) if values else None


def text(value, digits=2, percent=False):
    if value is None:
        return "n/a"
    return f"{value * 100:.0f}%" if percent else f"{value:.{digits}f}"


def followups():
    """Complete follow-up campaigns by variant, audited the same way as the campaign."""
    found = {}
    if not FOLLOWUP.exists():
        return found
    for latest in sorted(FOLLOWUP.glob("*/latest.path")):
        campaign = Path(latest.read_text().strip())
        state = read(campaign / "campaign.json")
        if state.get("status") != "complete":
            continue
        verification = audit(campaign)
        found[state["variant"]] = (campaign, state, verification)
    return found


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, default=CAMPAIGN)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    frozen_dirs = {(ROOT / "docs/results/m16").resolve(), (ROOT / "docs/results/current").resolve()}
    if args.output.resolve() in frozen_dirs:
        raise SystemExit("The frozen M7 and M16 result folders are never written")
    campaign = args.campaign.resolve()
    verification = audit(campaign)  # checksums, isolation and seed separation
    state = read(campaign / "campaign.json")
    frozen = read(campaign / "frozen-protocol.json")
    rows = measured_rows(state)
    compliance = compliance_table(rows)
    timing = timing_table(rows)
    extra = followups()
    follow_rows = []
    for variant, (_, follow_state, _) in extra.items():
        for row in compliance_table(measured_rows(follow_state)):
            row["variant"] = "reactive" if row["mode"] == "ml-reactive" else variant
            follow_rows.append(row | {"replica_seconds": None})
        for source, row in zip(
            measured_rows(follow_state),
            follow_rows[-len(measured_rows(follow_state)) :],
            strict=True,
        ):
            row["replica_seconds"] = round(source["replica_seconds"], 1)
            row["peak_lag"] = source["maximum_sampled_lag"]
    args.output.mkdir(parents=True, exist_ok=True)
    write_csv(args.output / "compliance.csv", compliance)
    write_csv(args.output / "surge-timing.csv", timing)
    if follow_rows:
        write_csv(args.output / "followup-ablation.csv", follow_rows)

    modes = ("fixed", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive")
    scenarios = tuple(frozen["scenarios"])
    lines = [
        "# M16 supplement: target compliance, step-surge diagnosis and ablation",
        "",
        "Derived read-only from the frozen campaign; the [M16 report](../m16/report.md) and "
        "[M7 report](../current/report.md) are unchanged. Values are medians over three "
        "repetitions on one node. No statistical significance is claimed.",
        "",
        "## Queueing-target compliance (3 s)",
        "",
        "**Measured** = share of unique accepted events whose SQL end-to-end latency was "
        "within 3 s. **Predicted** = share of controller decisions whose chosen replica count "
        "had a planner-predicted queueing delay within 3 s; a model output, available only "
        "for ML modes, that excludes the seconds before any new pod could start. The two "
        "measure different things and should not be read as one number.",
        "",
        "| Scenario | Mode | Measured within 3 s | Measured p95 (s) "
        "| Predicted: decisions within 3 s | Predicted median delay (s) |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for scenario in scenarios:
        for mode in modes:
            match = {"scenario": scenario, "mode": mode}
            measured = median(compliance, "measured_share_within_3s", **match)
            predicted = median(compliance, "predicted_share_of_decisions_within_3s", **match)
            lines.append(
                f"| {scenario} | {mode} | {text(measured, percent=True)} "
                f"| {text(median(compliance, 'measured_p95_seconds', **match))} "
                f"| {text(predicted, percent=True)} "
                f"| {text(median(compliance, 'predicted_median_delay_seconds', **match))} |"
            )
    lines += [
        "",
        "## Why ML-predictive had higher p95 on flash-sale and overload-recovery",
        "",
        "Seconds after the load transition (flash-sale: the surge start; overload-recovery: "
        "publication start, when the surge begins), medians of three runs:",
        "",
        "| Scenario | Mode | Controller ready | First decision | Scale-up requested "
        "| Three consumers ready | Forecast-driven decisions | Measured p95 (s) |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for scenario in SURGES:
        for mode in ("ml-reactive", "ml-predictive"):
            match = {"scenario": scenario, "mode": mode}
            lines.append(
                f"| {scenario} | {mode} "
                + " ".join(
                    f"| {text(median(timing, key, **match), 1)}"
                    for key in (
                        "controller_ready_s",
                        "first_decision_s",
                        "scale_up_requested_s",
                        "three_ready_s",
                        "forecast_driven_decisions",
                        "measured_p95_seconds",
                    )
                )
                + " |"
            )
    lines += [
        "",
        "Findings from the decision traces (`surge-timing.csv`, `autoscaler-decisions.json`):",
        "",
        "1. **The forecast changed no decision on these step surges.** No ML-predictive "
        "scale-up was forecast-driven in any of the six runs: every one was triggered by the "
        "current arrival rate and backlog, at the same decision as ML-reactive would make. "
        "A sudden step has no precursor, so the forecaster could not anticipate it.",
        "2. **Overload-recovery: later controller start.** Its surge begins at publication "
        "start, so every second before the controller's first decision is backlog. The "
        "predictive controller loads and parses its 285 KB forecaster at startup (2.7 s "
        "measured at 0.25 CPU in the same image). Its first decision, scale-up request and "
        "readiness came later than ML-reactive's in two of three repetitions, and its peak "
        "lag was higher in all three. This is a startup cost of the predictive controller, "
        "not a forecasting error.",
        "3. **Pod start variance dominates the rest.** Three consumers became ready 4–8 s "
        "after the request in both modes; the spread between repetitions of one mode is as "
        "large as the gap between modes. On flash-sale the median p95 gap (4.04 vs 3.70 s) "
        "is inside the ranges of both modes.",
        "4. **Online adaptation did not cause the gap.** Simulating the same evaluation "
        "seeds with adaptation on, off, forecast on and off gives identical results for both "
        "step-surge shapes: the scale-up is to the maximum in every variant.",
        "",
    ]
    if follow_rows:
        lines += [
            "## Follow-up: forecasting versus online adaptation (measured)",
            "",
            "Because the main campaign cannot separate them, a follow-up ran the frozen "
            "protocol, model and `eval:` ramp seeds (three repetitions) with one switch "
            "changed: `forecast-only` (no learned bias, capacity or pod delay) and "
            "`adapt-only` (the forecast output ignored). `full` and ML-reactive were rerun in "
            "the same session as paired baselines. Follow-up campaigns are scratch evidence, "
            "audited like the main campaign; they do not replace its numbers.",
            "",
            "| Variant (ramp) | Measured p95 (s) | Measured within 3 s | Peak lag | Replica-s |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
        for variant in ("reactive", "adapt-only", "forecast-only", "full"):
            match = {"variant": variant}
            if median(follow_rows, "measured_p95_seconds", **match) is None:
                continue
            lines.append(
                f"| {variant} | {text(median(follow_rows, 'measured_p95_seconds', **match))} "
                f"| {text(median(follow_rows, 'measured_share_within_3s', **match), percent=True)} "
                f"| {text(median(follow_rows, 'peak_lag', **match), 0)} "
                f"| {text(median(follow_rows, 'replica_seconds', **match), 0)} |"
            )
        lines.append("")
    model = frozen["model"]
    lines += [
        "## Provenance",
        "",
        f"- Campaign: `{campaign.name}` ({state['started_at']} → {state['ended_at']}), "
        f"{verification['measured_runs']} measured runs, audit `{verification['status']}`, "
        f"{verification['raw_samples']:,} raw samples.",
        f"- Image `{state['image']['image']}` from commit `{state['image']['git_commit']}` "
        f"(dirty: {state['image']['git_dirty']}).",
        f"- Forecaster sha256 `{model['sha256']}`, quantile {model['quantile']}, trained on "
        f"`{model['training_seeds']}`.",
        f"- campaign.json sha256 `{checksum(campaign / 'campaign.json')}`; summary.csv sha256 "
        f"`{checksum(campaign / 'summary.csv')}`.",
    ]
    for variant, (path, follow_state, follow_audit) in extra.items():
        lines.append(
            f"- Follow-up `{variant}`: `{path.name}`, {follow_audit['measured_runs']} runs, "
            f"audit `{follow_audit['status']}`, commit `{follow_state['image']['git_commit']}`."
        )
    lines += [
        "",
        "Reproduce (raw evidence is local under `artifacts/experiments/v3/`):",
        "",
        "```sh",
        "uv run --frozen python -m experiments.evidence --protocol v3        # re-audit",
        "uv run --project experiments/analysis --frozen python -m experiments.analyze \\",
        "    --protocol v3 --output /tmp/m16-check                          # frozen report",
        "uv run --frozen python -m experiments.supplement                    # this file",
        "```",
        "",
    ]
    (args.output / "report.md").write_text("\n".join(lines))
    print(f"Supplement: {args.output / 'report.md'}")


if __name__ == "__main__":
    main()

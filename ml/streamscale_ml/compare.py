"""Simulated comparison of every available mode on every v3 shape (training seeds only)."""

import argparse
import json
import statistics

from streamscale_workload.shapes import SHAPES, require_namespace, sale_starts, schedule

from streamscale_ml.calibrate import load
from streamscale_ml.policies import MODES, controller, pilot
from streamscale_ml.sim import run

METRICS = ("p95_latency", "peak_lag", "replica_seconds", "scale_changes")


def evaluate(modes, shapes, seeds, cluster, capacity, rates, model=None):
    rows = []
    for shape in shapes:
        for mode in modes:
            results = []
            for seed in seeds:
                phases = schedule(shape, seed, rates["steady"], rates["surge"])
                starts = sale_starts(phases, rates["steady"])
                result = run(phases, controller(mode, capacity, model), cluster, starts)
                results.append(result)
            row = {"shape": shape, "mode": mode, "runs": len(results)}
            for key in METRICS:
                row[key] = round(statistics.median(getattr(item, key) for item in results), 2)
            ready = [value for item in results for value in item.ready_at_sale_start]
            row["mean_ready_at_sale_start"] = round(statistics.mean(ready), 2) if ready else None
            rows.append(row)
    return rows


def table(rows):
    header = f"{'shape':16} {'mode':14} {'p95 s':>7} {'peak lag':>9} {'replica·s':>10} "
    header += f"{'changes':>8} {'ready@sale':>10}"
    lines = [header]
    for row in rows:
        ready = row["mean_ready_at_sale_start"]
        lines.append(
            f"{row['shape']:16} {row['mode']:14} {row['p95_latency']:7.2f} {row['peak_lag']:9.0f}"
            f" {row['replica_seconds']:10.0f} {row['scale_changes']:8.0f}"
            f" {'' if ready is None else f'{ready:.2f}':>10}"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    rates, capacity = pilot()
    seeds = [require_namespace(f"train:{index}", "train") for index in range(args.seeds)]
    from streamscale.autoscale.forecast import load as load_model

    rows = evaluate(MODES, SHAPES, seeds, load(), capacity, rates, load_model())
    print(json.dumps(rows, indent=2) if args.json else table(rows))


if __name__ == "__main__":
    main()

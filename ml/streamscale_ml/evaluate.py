"""Offline evaluation of all five modes on evaluation and stress seeds (simulation only).

Evaluation seeds (``eval:N``) are never used for fitting or tuning. Stress seeds draw sale
gaps, lengths and peaks outside the training ranges. Results are descriptive simulator
outcomes, not measurements; the measured campaign is the evidence of record.
"""

import argparse
import json
from pathlib import Path

from streamscale_workload.shapes import SHAPES

from streamscale.autoscale.forecast import load as load_model
from streamscale_ml.calibrate import load
from streamscale_ml.compare import evaluate, table
from streamscale_ml.policies import MODES, pilot

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "artifacts/ml/offline-eval.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=int, default=10)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    rates, capacity = pilot()
    cluster = load()
    model = load_model()
    report = {
        "note": "simulated outcomes; not measured evidence",
        "model_sha256": model.sha256,
        "quantile": model.quantile,
        "simulator": cluster.to_dict(),
        "sets": {},
    }
    for namespace in ("eval", "stress"):
        seeds = [f"{namespace}:{index}" for index in range(args.seeds)]
        rows = evaluate(MODES, SHAPES, seeds, cluster, capacity, rates, model)
        report["sets"][namespace] = rows
        print(f"\n{namespace} seeds ({args.seeds} per shape)")
        print(table(rows))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"\nWrote {args.output}")


if __name__ == "__main__":
    main()

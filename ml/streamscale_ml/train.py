"""Train the quantile gradient-boosted forecaster on simulated training-seed traces.

Each trace is a closed-loop simulation of a training-seed shape under a baseline controller,
with steady and surge rates drawn per seed so the model learns capacity-relative patterns.
At every second the shared feature code computes inputs; targets are the peak arrival rate
in four future windows. Models are exported to JSON for the plain-Python evaluator.
"""

import argparse
import hashlib
import json
import random
import time
from pathlib import Path

import numpy as np
import sklearn
from sklearn.ensemble import GradientBoostingRegressor
from streamscale_workload.shapes import (
    SHAPE_VERSION,
    even,
    require_namespace,
    sale_starts,
    schedule,
)

from streamscale.autoscale.features import FEATURES, ArrivalHistory
from streamscale.autoscale.forecast import FORMAT
from streamscale.autoscale.observation import Observation
from streamscale_ml.calibrate import load
from streamscale_ml.policies import controller, pilot
from streamscale_ml.sim import run

ROOT = Path(__file__).resolve().parents[2]
MODEL = ROOT / "src/streamscale/autoscale/models/forecaster-v1.json"
REPORT = ROOT / "ml/training-report.json"
WINDOWS = ((0, 4), (5, 9), (10, 14), (15, 19))
QUANTILE = 0.9
# New shapes are where forecasting can help, so they appear twice as often.
ROTATION = (
    "steady",
    "flash-sale",
    "overload-recovery",
    "ramp",
    "repeated-sales",
    "ramp",
    "repeated-sales",
)
TRACE_CONTROLLERS = ("fixed", "lag-keda", "ml-reactive")
MAX_EXTRA_LEAD = 120
HYPERPARAMETERS = {
    "n_estimators": 120,
    "max_depth": 3,
    "learning_rate": 0.1,
    "subsample": 0.8,
    "min_samples_leaf": 20,
}


def trace_setup(seed: str, capacity: float):
    """Shape, rates and baseline controller for one training seed."""
    require_namespace(seed, "train")
    rng = random.Random(f"trace-v1:{seed}")
    index = int(seed.split(":")[1])
    shape = ROTATION[index % len(ROTATION)]
    steady = capacity * rng.uniform(0.3, 0.5)
    surge = capacity * rng.uniform(2.5, 3.2)
    phases = schedule(shape, seed, steady, surge)
    # A variable quiet lead keeps the first sale unpredictable, so the model cannot learn the
    # protocol's fixed 15 s lead; only trends and rhythms within a run remain forecastable.
    lead = rng.randint(0, MAX_EXTRA_LEAD)
    if lead:
        phases = (
            [[lead, phases[0][1]]] + phases
            if shape != "overload-recovery"
            else ([[lead, even(steady)]] + phases)
        )
    mode = TRACE_CONTROLLERS[rng.randrange(len(TRACE_CONTROLLERS))]
    return shape, phases, round(steady), mode


def rows_for_trace(seed: str, capacity: float, cluster):
    """Feature rows and window targets (in capacities) for one simulated trace."""
    shape, phases, steady, mode = trace_setup(seed, capacity)
    simulated = controller(mode, capacity)
    result = run(phases, simulated, cluster, sale_starts(phases, steady))
    history = ArrivalHistory(capacity)
    total = 0.0
    features, targets = [], []
    arrivals = result.arrivals
    for second in range(len(arrivals)):
        # What the controller saw at the start of `second` (see sim.View).
        lag = result.lag[second - 1] if second else 0.0
        history.add(observation(second, total, lag, result.ready[second]))
        total += arrivals[second]
        row = history.features()
        if row is None:
            continue
        window_peaks = []
        for start, end in WINDOWS:
            future = arrivals[second + start : second + end + 1]
            window_peaks.append(max(future) / capacity if future else 0.0)
        features.append(row)
        targets.append(window_peaks)
    return shape, features, targets


def observation(second, total, lag, ready):
    third = int(total) // 3
    backlog = int(round(lag))
    return Observation(
        at=float(second),
        highs=(third, third, int(total) - 2 * third),
        lags=(backlog // 3, backlog // 3, backlog - 2 * (backlog // 3)),
        ready=ready,
        desired=max(1, ready),
        fresh_at=float(second),
    )


def dataset(seeds, capacity, cluster):
    features, targets, shapes = [], [], []
    for seed in seeds:
        shape, rows, values = rows_for_trace(seed, capacity, cluster)
        features.extend(rows)
        targets.extend(values)
        shapes.extend([shape] * len(rows))
    return np.asarray(features, dtype=np.float64), np.asarray(targets), shapes


def export_tree(estimator):
    tree = estimator.tree_
    return {
        "feature": [int(value) for value in tree.feature],
        "threshold": [float(value) for value in tree.threshold],
        "left": [int(value) for value in tree.children_left],
        "right": [int(value) for value in tree.children_right],
        "value": [float(value) for value in tree.value[:, 0, 0]],
    }


def fit_window(features, target, window, quantile=QUANTILE):
    model = GradientBoostingRegressor(
        loss="quantile", alpha=quantile, random_state=1600 + window[0], **HYPERPARAMETERS
    )
    model.fit(features, target)
    return model, {
        "window": list(window),
        "init": float(model.init_.constant_.ravel()[0]),
        "learning_rate": float(model.learning_rate),
        "trees": [export_tree(stage[0]) for stage in model.estimators_],
    }


def pinball(target, predicted, quantile=QUANTILE):
    error = target - predicted
    return float(np.mean(np.maximum(quantile * error, (quantile - 1) * error)))


def scores(target, predicted, persistence, quantile=QUANTILE):
    return {
        "pinball": round(pinball(target, predicted, quantile), 4),
        "persistence_pinball": round(pinball(target, persistence, quantile), 4),
        "coverage": round(float(np.mean(target <= predicted + 1e-9)), 4),
        "persistence_coverage": round(float(np.mean(target <= persistence + 1e-9)), 4),
    }


def train(traces, holdout, quantile, output: Path, metadata=None):
    """Fit and export one forecaster; returns the training report."""
    if not 0 < quantile < 1:
        raise ValueError("Quantile must be in (0, 1)")
    started = time.monotonic()
    _, capacity = pilot()
    cluster = load()
    train_seeds = [require_namespace(f"train:{index}", "train") for index in range(traces)]
    # Held out from fitting but still training-namespace: evaluation seeds stay untouched.
    holdout_seeds = [f"train:{100_000 + index}" for index in range(holdout)]
    features, targets, _ = dataset(train_seeds, capacity, cluster)
    held_features, held_targets, held_shapes = dataset(holdout_seeds, capacity, cluster)
    windows, quality = [], {}
    persistence = held_features[:, FEATURES.index("rate_1")]
    for index, window in enumerate(WINDOWS):
        model, exported = fit_window(features, targets[:, index], window, quantile)
        windows.append(exported)
        predicted = model.predict(held_features)
        label = f"{window[0]}-{window[1]}s"
        quality[label] = {"all": scores(held_targets[:, index], predicted, persistence, quantile)}
        for shape in sorted(set(held_shapes)):
            mask = np.asarray([item == shape for item in held_shapes])
            quality[label][shape] = scores(
                held_targets[mask, index], predicted[mask], persistence[mask], quantile
            )
    payload = {
        "format": FORMAT,
        "features": list(FEATURES),
        "quantile": quantile,
        "target": "peak arrivals per consumer capacity in each future window",
        "training_seeds": f"train:0..train:{traces - 1}",
        "holdout_seeds": f"train:100000..train:{100_000 + holdout - 1}",
        "shape_version": SHAPE_VERSION,
        "pilot_capacity": capacity,
        "simulator": cluster.to_dict(),
        "hyperparameters": HYPERPARAMETERS,
        "sklearn_version": sklearn.__version__,
        "rows": int(len(features)),
        **(metadata or {}),
        "windows": windows,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n"
    output.write_text(text)
    resolved = output.resolve()
    return {
        "model": str(resolved.relative_to(ROOT) if resolved.is_relative_to(ROOT) else output),
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "quantile": quantile,
        "rows": int(len(features)),
        "holdout_rows": int(len(held_features)),
        "seconds": round(time.monotonic() - started, 1),
        "holdout_quality": quality,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--traces", type=int, default=700)
    parser.add_argument("--holdout", type=int, default=140)
    parser.add_argument("--output", type=Path, default=MODEL)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--quantile", type=float, default=QUANTILE)
    args = parser.parse_args()
    if not 0 < args.quantile < 1:
        parser.error("--quantile must be in (0, 1)")
    report = train(args.traces, args.holdout, args.quantile, args.output)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print_report(report)


def print_report(report):
    print(json.dumps({key: report[key] for key in ("model", "sha256", "rows", "seconds")}))
    for label, values in report["holdout_quality"].items():
        overall = values["all"]
        print(
            f"window {label:7} pinball {overall['pinball']:.4f} "
            f"(persistence {overall['persistence_pinball']:.4f})  coverage "
            f"{overall['coverage']:.2f} (persistence {overall['persistence_coverage']:.2f})"
        )


if __name__ == "__main__":
    main()

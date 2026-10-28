import json

import numpy as np
import pytest
from streamscale_ml import train
from streamscale_ml.policies import controller, pilot
from streamscale_ml.sim import run
from streamscale_ml.tune import choose
from streamscale_workload.shapes import sale_starts, schedule

from streamscale.autoscale.features import FEATURES
from streamscale.autoscale.forecast import Forecaster, load
from streamscale.autoscale.observation import Observation
from streamscale.autoscale.planner import PlannerConfig
from streamscale.autoscale.policy import PredictivePolicy

RATES, CAPACITY = pilot()


@pytest.fixture(scope="module")
def small_model(tmp_path_factory):
    features, targets, _ = train.dataset([f"train:{i}" for i in range(14)], CAPACITY, train.load())
    fitted, exported = train.fit_window(features, targets[:, 1], (5, 9), 0.8)
    payload = {
        "format": "streamscale-gbt-quantile-v1",
        "features": list(FEATURES),
        "quantile": 0.8,
        "windows": [exported],
    }
    return fitted, payload, features


def test_plain_python_trees_match_scikit_learn(small_model):
    fitted, payload, features = small_model
    forecaster = Forecaster(payload, "test")
    expected = fitted.predict(features[:500])
    actual = np.asarray([forecaster.predict(row, 1.0)[0] for row in features[:500]])
    assert np.allclose(np.maximum(expected, 0), actual, atol=1e-9)


def test_feature_order_and_format_are_checked(small_model):
    _, payload, _ = small_model
    with pytest.raises(ValueError, match="feature order"):
        Forecaster({**payload, "features": list(reversed(FEATURES))}, "x")
    with pytest.raises(ValueError, match="Unsupported"):
        Forecaster({**payload, "format": "pickle"}, "x")
    with pytest.raises(ValueError, match="length"):
        Forecaster(payload, "x").predict([0.0], 1.0)


def test_corrupt_model_file_fails_loudly(tmp_path):
    broken = tmp_path / "model.json"
    broken.write_text("{not json")
    with pytest.raises(ValueError, match="not valid JSON"):
        load(broken)


def test_packaged_model_records_training_provenance():
    model = load()
    assert model.metadata["training_seeds"].startswith("train:")
    assert "eval" not in json.dumps(model.metadata["training_seeds"])
    assert model.quantile == model.metadata["tuning"]["chosen_quantile"]
    assert [tuple(window) for window in model.horizons] == list(train.WINDOWS)


def test_training_refuses_evaluation_seeds():
    with pytest.raises(ValueError, match="not a train seed"):
        train.trace_setup("eval:3", CAPACITY)


def test_tuning_prefers_fewer_replica_seconds_within_the_tie():
    rows = [
        {"quantile": 0.9, "target_met": 0.80, "median_replica_seconds": 190},
        {"quantile": 0.8, "target_met": 0.79, "median_replica_seconds": 150},
        {"quantile": 0.5, "target_met": 0.60, "median_replica_seconds": 120},
    ]
    assert choose(rows)["quantile"] == 0.8


def test_learned_estimates_stay_within_bounds():
    policy = PredictivePolicy(PlannerConfig(), load())
    for second in range(200):
        policy.step(
            Observation(
                float(second),
                (second * 1000,) * 3,
                (5000, 5000, 5000),
                1,
                1,
                float(second),
            )
        )
    learned = policy.learned()
    assert 0.6 * CAPACITY - 1 <= learned["capacity"] <= 1.4 * CAPACITY + 1
    assert 1.0 <= learned["ready_delay"] <= 20.0
    assert abs(learned["bias"]) <= 0.5 * CAPACITY + 1


def test_predictive_adds_consumers_before_a_repeated_sale():
    phases = schedule("repeated-sales", "eval:1", RATES["steady"], RATES["surge"])
    starts = sale_starts(phases, RATES["steady"])
    predictive = run(phases, controller("ml-predictive", CAPACITY, load()), train.load(), starts)
    reactive = run(phases, controller("ml-reactive", CAPACITY), train.load(), starts)
    assert predictive.p95_latency <= reactive.p95_latency
    assert sum(predictive.ready_at_sale_start) >= sum(reactive.ready_at_sale_start)

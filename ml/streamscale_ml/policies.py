"""Simulated controllers for every M16 mode, built the way the cluster builds them."""

import json
from pathlib import Path

from streamscale.autoscale.planner import PlannerConfig
from streamscale.autoscale.policy import build_policy
from streamscale_ml.sim import CpuHpaLike, FixedController, KedaLike, PolicyController

ROOT = Path(__file__).resolve().parents[2]
M7_FROZEN = ROOT / "docs/results/current/frozen-protocol.json"
MODES = ("fixed", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive")


def pilot():
    """Frozen M7 rates and pilot capacity; the policy knows capacity only through this."""
    frozen = json.loads(M7_FROZEN.read_text())
    return frozen["rates"], float(frozen["pilot_capacity_per_second"])


def planner_config(capacity, **overrides):
    return PlannerConfig(capacity_per_consumer=capacity, **overrides)


def controller(mode, capacity, model=None, **overrides):
    if mode == "fixed":
        return FixedController()
    if mode == "cpu-hpa":
        return CpuHpaLike()
    if mode == "lag-keda":
        return KedaLike()
    if mode == "ml-reactive":
        config = planner_config(capacity, **overrides)
        return PolicyController(lambda: build_policy("reactive", config), mode)
    if mode == "ml-predictive":
        if model is None:
            raise ValueError("ml-predictive needs a forecaster model")
        config = planner_config(capacity, **overrides)
        return PolicyController(lambda: build_policy("predictive", config, model), mode)
    raise ValueError(f"Unknown mode {mode!r}")

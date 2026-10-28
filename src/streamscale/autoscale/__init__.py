"""Replica controller for the ml-predictive and ml-reactive scaling modes.

The core is plain Python so the measured image, the console agent and the offline simulator
share exactly the same decision code. It only sees Kafka offsets, lag and replica counts; it
never reads a run's planned schedule.
"""

from streamscale.autoscale.observation import Observation, parse_exporter
from streamscale.autoscale.planner import Decision, Planner, PlannerConfig
from streamscale.autoscale.policy import ReactivePolicy, build_policy

__all__ = [
    "Decision",
    "Observation",
    "Planner",
    "PlannerConfig",
    "ReactivePolicy",
    "build_policy",
    "parse_exporter",
]

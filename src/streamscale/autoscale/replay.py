"""Open-loop replay: feed a recorded or synthetic observation trace through a policy.

Replay does not apply decisions to the trace. It shows what a policy would have chosen and
why, which is enough to inspect planner behaviour without a cluster.
"""

import json
from pathlib import Path

from streamscale.autoscale.observation import PARTITIONS, Observation
from streamscale.autoscale.planner import ASSIGNMENTS, PlannerConfig
from streamscale.autoscale.policy import build_policy

# M7 frozen v2 shapes (pilot 130.5/s): steady 52/s, surge 390/s.
SYNTHETIC = {
    "steady": [[60, 52]],
    "flash-sale": [[15, 52], [20, 390], [25, 52]],
    "overload-recovery": [[35, 390], [25, 52]],
}


def open_loop_trace(schedule, capacity=130.0, replicas=1, drain_seconds=30):
    """Observations of a fixed-``replicas`` consumer group serving ``schedule``."""
    queues = [0.0] * PARTITIONS
    highs = [0.0] * PARTITIONS
    rates = [rate for seconds, rate in schedule for _ in range(seconds)]
    rates += [0] * drain_seconds
    trace = []
    for second, rate in enumerate(rates, start=1):
        for part in range(PARTITIONS):
            queues[part] += rate / PARTITIONS
            highs[part] += rate / PARTITIONS
        for group in ASSIGNMENTS[replicas]:
            total = sum(queues[part] for part in group)
            if total > 0:
                served = min(total, capacity)
                for part in group:
                    queues[part] -= served * queues[part] / total
        trace.append(
            Observation(
                at=float(second),
                highs=tuple(int(value) for value in highs),
                lags=tuple(int(round(value)) for value in queues),
                ready=replicas,
                desired=replicas,
                fresh_at=float(second),
            )
        )
    return trace


def read_trace(path: Path):
    return [
        Observation.from_dict(json.loads(line))
        for line in path.read_text().splitlines()
        if line.strip()
    ]


def replay(trace, policy_name="reactive", config: PlannerConfig | None = None):
    policy = build_policy(policy_name, config or PlannerConfig())
    return [policy.step(observation) for observation in trace]


def summarize(decisions, policy_name):
    proposals = []
    previous = None
    for decision in decisions:
        if decision.replicas != previous:
            proposals.append(decision.to_dict())
            previous = decision.replicas
    return {
        "policy": policy_name,
        "steps": len(decisions),
        "max_replicas": max((item.replicas for item in decisions), default=None),
        "proposals": proposals,
    }

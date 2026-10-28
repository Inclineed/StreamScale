"""ml-reactive / ml-predictive control loop for the console consumers.

Runs the same controller core as the measured autoscaler Deployment
(``streamscale.autoscale``), fed from the agent observer's Kafka sample: per-partition
high watermarks and lag for the current sale's group, plus the console Deployment's
desired and ready counts. It never reads the sale schedule or the planned rate.

The loop acts only while the scaling mode is an ML mode, no manual override is active and
no scaling transition is running. It takes the scaling component's transition lock for
each write, so it can never race a mode switch or a fault-lab change.
"""

import json
import threading
import time
from collections import deque

from streamscale.autoscale.observation import PARTITIONS, Observation
from streamscale.autoscale.planner import PlannerConfig
from streamscale.autoscale.policy import build_policy
from streamscale_console_worker.scaling import AGENT_OWNED

# Matches the measured pilot capacity and the M16 frozen controller settings.
CONSOLE_CONFIG = PlannerConfig(capacity_per_consumer=130.5, target_delay_seconds=3.0)
KEEP = 120
STALE_SECONDS = 5.0


def observation_from(sample):
    """Build a controller observation from an observer sample, or explain why not."""
    kafka = sample.get("kafka") or {}
    partitions = kafka.get("partitions") or []
    deployment = (sample.get("kubernetes") or {}).get("consumer") or {}
    run = sample.get("run") or {}
    if len(partitions) != PARTITIONS or any(row.get("lag") is None for row in partitions):
        return None, "waiting for a sale's Kafka offsets"
    if not (sample.get("sources") or {}).get("kafka", {}).get("ok", False):
        return None, "Kafka sample unavailable; holding"
    if "desired" not in deployment:
        return None, "console Deployment status unavailable"
    return (
        Observation(
            at=float(sample["sampled_at"]),
            highs=tuple(int(row["high"]) for row in partitions),
            lags=tuple(int(row["lag"]) for row in partitions),
            ready=int(deployment.get("ready") or 0),
            desired=int(deployment["desired"]),
            fresh_at=float(sample["sampled_at"]),
        ),
        run.get("run_id"),
    )


class Autoscale:
    def __init__(self, agent, config=CONSOLE_CONFIG, clock=time.time, forecaster=None):
        self.agent = agent
        self.kube = agent.kube
        self.scaling = agent.components["scaling"]
        self.config = config
        self.clock = clock
        self.forecaster = forecaster
        self.policy = None
        self.key = None
        self.last_sequence = None
        self.decisions = deque(maxlen=KEEP)
        self.status = "idle"
        self.lock = threading.Lock()
        self.runners = [self.loop]
        self.sections = {"autoscaler": self.section}
        # Keep the observer at 1 Hz while an ML mode owns replicas.
        self.busy = [self.active]

    def active(self):
        state = self.scaling.state
        return state["mode"] in AGENT_OWNED and not state["override"]

    def mode(self):
        return self.scaling.state["mode"]

    def loop(self, stop):
        while not stop.wait(1.0):
            try:
                self.tick()
            except Exception as error:  # noqa: BLE001 - reported, never silently ignored
                with self.lock:
                    self.status = f"error: {type(error).__name__}: {str(error)[:160]}"

    def tick(self):
        if not self.active():
            with self.lock:
                self.status = "idle" if self.mode() not in AGENT_OWNED else "suspended by override"
                self.policy = None
                self.key = None
            return None
        if self.scaling.state["transition"]["state"] == "running":
            with self.lock:
                self.status = "waiting for the scaling transition"
            return None
        sample = self.agent.observer.latest()
        if sample is None or sample.get("sequence") == self.last_sequence:
            return None
        self.last_sequence = sample.get("sequence")
        if self.clock() - sample["sampled_at"] > STALE_SECONDS:
            with self.lock:
                self.status = "observer sample is stale; holding"
            return None
        observation, run_id = observation_from(sample)
        if observation is None:
            with self.lock:
                self.status = run_id
            return None
        key = (self.mode(), run_id)
        if key != self.key or self.policy is None:
            policy = self.mode().removeprefix("ml-")
            self.policy = build_policy(policy, self.config, self.forecaster)
            self.key = key
        decision = self.policy.step(observation)
        applied = False
        if decision.replicas != observation.desired:
            # The transition lock serializes with mode switches, overrides and faults.
            if self.scaling.work.acquire(blocking=False):
                try:
                    if self.active():
                        self.kube.scale_consumers(decision.replicas)
                        applied = True
                finally:
                    self.scaling.work.release()
        record = decision.to_dict() | {
            "run_id": run_id,
            "ready": observation.ready,
            "lag": observation.lag,
            "applied": applied,
            "mode": self.mode(),
        }
        with self.lock:
            self.decisions.append(record)
            self.status = "controlling"
        return record

    def section(self):
        with self.lock:
            recent = list(self.decisions)[-30:]
            policy = self.policy.describe() if self.policy is not None else None
            status = self.status
        if policy is not None:
            policy = json.loads(json.dumps(policy))
            policy.pop("planner", None)
        return {
            "status": status,
            "policy": policy,
            "target_delay_seconds": self.config.target_delay_seconds,
            "capacity_per_consumer": self.config.capacity_per_consumer,
            "last": recent[-1] if recent else None,
            "recent": recent,
        }


def component(agent):
    return Autoscale(agent)

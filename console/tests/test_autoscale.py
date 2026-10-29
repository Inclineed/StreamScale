"""Console ML modes: agent-owned replicas, override and transition interplay."""

import threading
import time

import pytest
from streamscale_console_worker.autoscale import Autoscale, observation_from
from streamscale_console_worker.scaling import (
    AGENT_OWNED,
    CONTROLLED,
    Scaling,
    ScalingError,
    expected_controllers,
)

from .test_scaling import FakeKube
from .test_scaling import wait as wait_state


def wait(scaling):
    """Wait for the transition to finish *and* release its lock (set after the state)."""
    transition = wait_state(scaling)
    deadline = time.monotonic() + 5
    while not scaling.work.acquire(timeout=0.05):
        assert time.monotonic() < deadline
    scaling.work.release()
    return transition


class FakeObserver:
    def __init__(self):
        self.samples = []

    def latest(self):
        return self.samples[-1] if self.samples else None


class FakeAgent:
    def __init__(self, kube):
        self.kube = kube
        self.components = {}
        self.observer = FakeObserver()


def sample(sequence, at, high, lag, desired=1, ready=1, run_id="run-1", ok=True):
    third = high // 3
    return {
        "sequence": sequence,
        "sampled_at": at,
        "sources": {"kafka": {"ok": ok}},
        "run": {"run_id": run_id},
        "kafka": {
            "partitions": [{"partition": part, "high": third, "lag": lag // 3} for part in range(3)]
        },
        "kubernetes": {"consumer": {"desired": desired, "ready": ready}},
    }


@pytest.fixture
def setup(tmp_path):
    kube = FakeKube(replicas=1)
    agent = FakeAgent(kube)
    scaling = Scaling(agent, root=tmp_path, sleep=lambda seconds: None)
    scaling.startup(threading.Event())
    agent.components["scaling"] = scaling
    now = [1000.0]
    autoscale = Autoscale(agent, clock=lambda: now[0])
    return scaling, autoscale, agent, kube, now


def test_ml_modes_own_replicas_without_cluster_controllers():
    for mode in AGENT_OWNED:
        assert expected_controllers(mode, None) == set()
        assert mode in CONTROLLED


def test_switching_to_ml_removes_autoscalers_and_keeps_bounds(tmp_path):
    kube = FakeKube(keda=True, replicas=3)
    scaling = Scaling(FakeAgent(kube), root=tmp_path, sleep=lambda seconds: None)
    scaling.startup(threading.Event())
    scaling.set_mode({"mode": "ml-predictive"})
    assert wait(scaling)["state"] == "succeeded"
    assert not kube.keda and not kube.hpa and kube.replicas == 3
    assert scaling.section()["ownership"]["consistent"]


def test_controller_scales_on_backlog_and_records_its_reason(setup):
    scaling, autoscale, agent, kube, now = setup
    scaling.set_mode({"mode": "ml-reactive"})
    wait(scaling)
    assert autoscale.active()
    agent.observer.samples.append(sample(1, 999.5, 0, 0))
    autoscale.tick()
    agent.observer.samples.append(sample(2, 1000.0, 390, 270))
    record = autoscale.tick()
    assert record["replicas"] == 3 and record["applied"]
    assert kube.replicas == 3
    assert "target" in record["reason"]
    section = autoscale.section()
    assert section["last"]["replicas"] == 3 and section["status"] == "controlling"


def test_override_and_running_transitions_suspend_the_loop(setup):
    scaling, autoscale, agent, kube, now = setup
    scaling.set_mode({"mode": "ml-reactive"})
    wait(scaling)
    scaling.enter_override({"replicas": 2})
    wait(scaling)
    assert not autoscale.active()
    agent.observer.samples.append(sample(1, 1000.0, 3900, 3000, desired=2, ready=2))
    assert autoscale.tick() is None
    assert kube.replicas == 2
    assert autoscale.section()["status"] == "suspended by override"
    scaling.release_override({})
    wait(scaling)
    scaling.state["transition"]["state"] = "running"
    agent.observer.samples.append(sample(2, 1000.0, 7800, 6000, desired=2, ready=2))
    assert autoscale.tick() is None
    assert kube.replicas == 2


def test_stale_or_missing_kafka_data_holds(setup):
    scaling, autoscale, agent, kube, now = setup
    scaling.set_mode({"mode": "ml-predictive"})
    wait(scaling)
    agent.observer.samples.append(sample(1, 900.0, 0, 0))
    assert autoscale.tick() is None
    assert "stale" in autoscale.section()["status"]
    missing = sample(2, 1000.0, 0, 0)
    for row in missing["kafka"]["partitions"]:
        row["lag"] = None
    assert observation_from(missing)[0] is None
    assert observation_from(sample(3, 1000.0, 0, 0, ok=False))[0] is None


def test_new_sale_resets_the_controller(setup):
    scaling, autoscale, agent, kube, now = setup
    scaling.set_mode({"mode": "ml-reactive"})
    wait(scaling)
    agent.observer.samples.append(sample(1, 1000.0, 900, 0, run_id="a"))
    autoscale.tick()
    first = autoscale.policy
    # A new sale's offsets are larger; a new run must not look like a continuation.
    agent.observer.samples.append(sample(2, 1000.0, 300, 0, run_id="b"))
    autoscale.tick()
    assert autoscale.policy is not first


def test_override_is_allowed_from_ml_modes_but_replicas_stay_controller_owned(setup):
    scaling, *_ = setup
    scaling.set_mode({"mode": "ml-predictive"})
    wait(scaling)
    with pytest.raises(ScalingError):
        scaling.set_replicas({"replicas": 2})
    scaling.enter_override({"replicas": 1})
    assert wait(scaling)["state"] == "succeeded"
    assert scaling.state["override"]["base"] == "ml-predictive"


def test_loop_reports_errors_instead_of_dying(setup):
    scaling, autoscale, agent, kube, now = setup
    scaling.set_mode({"mode": "ml-reactive"})
    wait(scaling)
    agent.observer.samples.append({"sequence": 1, "sampled_at": 1000.0})
    stop = threading.Event()
    thread = threading.Thread(target=autoscale.loop, args=(stop,), daemon=True)
    thread.start()
    deadline = time.monotonic() + 3
    while not autoscale.section()["status"].startswith(("error", "waiting")):
        assert time.monotonic() < deadline
        time.sleep(0.05)
    stop.set()

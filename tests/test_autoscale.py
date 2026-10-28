import json

import pytest

from streamscale.autoscale.controller import Controller
from streamscale.autoscale.features import FEATURES, NEVER, ArrivalHistory
from streamscale.autoscale.kube import DeploymentScaler
from streamscale.autoscale.observation import Observation, exporter_sample, parse_exporter
from streamscale.autoscale.planner import Planner, PlannerConfig, simulate
from streamscale.autoscale.policy import ReactivePolicy, build_policy
from streamscale.autoscale.replay import SYNTHETIC, open_loop_trace, replay

CONFIG = PlannerConfig()


def observation(at, high, lag=0, ready=1, desired=None, fresh_at=None):
    per = high // 3
    lags = (lag // 3, lag // 3, lag - 2 * (lag // 3))
    return Observation(
        at=float(at),
        highs=(per, per, high - 2 * per),
        lags=lags,
        ready=ready,
        desired=ready if desired is None else desired,
        fresh_at=float(at) if fresh_at is None else fresh_at,
    )


def test_observation_requires_three_partitions_and_nonnegative_values():
    with pytest.raises(ValueError, match="3 partitions"):
        Observation(0, (1, 2), (0, 0), 1, 1)
    with pytest.raises(ValueError, match="nonnegative"):
        Observation(0, (1, 2, 3), (0, -1, 0), 1, 1)


def test_exporter_text_parses_highs_lags_and_freshness():
    payload = "\n".join(
        [
            *(
                f'streamscale_kafka_lag{{run_id="r",mode="ml-reactive",scenario="steady",'
                f'partition="{part}"}} {part * 2}'
                for part in range(3)
            ),
            *(
                f'streamscale_kafka_high_watermark{{run_id="r",mode="ml-reactive",'
                f'scenario="steady",partition="{part}"}} {100 + part}'
                for part in range(3)
            ),
            'streamscale_exporter_last_success_unixtime{run_id="r",mode="ml-reactive",'
            'scenario="steady"} 12.5',
        ]
    )
    assert parse_exporter(payload, "r") == ((100, 101, 102), (0, 2, 4), 12.5)
    sample = exporter_sample(payload)
    assert (sample.run_id, sample.mode, sample.scenario) == ("r", "ml-reactive", "steady")
    with pytest.raises(ValueError, match="all partitions"):
        parse_exporter(payload, "other-run")
    mixed = payload.replace(
        'run_id="r",mode="ml-reactive",scenario="steady"} 12.5', 'run_id="x"} 1'
    )
    with pytest.raises(ValueError, match="more than one run"):
        exporter_sample(mixed)


def test_arrival_rates_come_from_high_watermark_differences():
    history = ArrivalHistory(130)
    for second in range(11):
        history.add(observation(second, 52 * second))
    assert history.rate(1) == pytest.approx(52)
    assert history.rate(5) == pytest.approx(52)
    with pytest.raises(ValueError, match="backwards"):
        history.add(observation(12, 0))


def test_sale_detection_and_features_use_only_observed_offsets():
    history = ArrivalHistory(130)
    high = 0
    for second in range(60):
        rate = 390 if 15 <= second < 35 else 52
        high += rate
        history.add(observation(second, high))
    features = dict(zip(FEATURES, history.features(), strict=True))
    assert features["sales_seen"] == 1
    assert features["in_sale"] == 0
    assert 18 <= features["last_sale_seconds"] <= 22
    assert features["last_gap_seconds"] == NEVER
    assert features["rate_1"] == pytest.approx(52 / 130)


def test_queue_model_needs_three_consumers_for_a_390_surge():
    rates = [390] * CONFIG.horizon_seconds
    assert simulate((0, 0, 0), rates, 3, 3, 0, CONFIG) < 1
    assert simulate((0, 0, 0), rates, 1, 2, 0, CONFIG) > CONFIG.target_delay_seconds
    assert simulate((0, 0, 0), rates, 1, 1, 0, CONFIG) > 30


def test_pod_start_delay_counts_against_a_scale_up():
    rates = [390] * CONFIG.horizon_seconds
    quick = simulate((0, 0, 0), rates, 1, 3, 0, CONFIG)
    slow = simulate((0, 0, 0), rates, 1, 3, 10, CONFIG)
    assert slow > quick


def test_surge_scales_to_three_within_one_step():
    policy = ReactivePolicy(CONFIG)
    assert policy.step(observation(0, 0)).replicas == 1
    assert policy.step(observation(1, 52)).replicas == 1
    decision = policy.step(observation(2, 52 + 390, lag=260))
    assert decision.replicas == 3
    assert decision.changed
    assert "target" in decision.reason


def test_scale_in_waits_for_hold_and_interval():
    policy = ReactivePolicy(CONFIG)
    high = 0
    replicas = 1
    changes = []
    for second in range(80):
        rate = 390 if 5 <= second < 15 else 52
        high += rate
        decision = policy.step(observation(second, high, ready=replicas, desired=replicas))
        if decision.replicas != replicas:
            changes.append((second, decision.replicas))
        replicas = decision.replicas
    assert changes[0][1] > 1
    peak = next(second for second, value in changes if value == 3)
    assert peak <= 7
    down = [second for second, value in changes if value < 3 and second > peak]
    assert down, "the policy never scaled back in"
    assert down[0] - peak >= CONFIG.scale_down_interval_seconds
    assert all(1 <= value <= 3 for _, value in changes)


def test_stale_exporter_samples_hold_the_current_count():
    policy = ReactivePolicy(CONFIG)
    policy.step(observation(0, 0, ready=2, desired=2, fresh_at=0))
    decision = policy.step(observation(10, 3900, lag=3000, ready=2, desired=2, fresh_at=0))
    assert decision.replicas == 2
    assert "stale" in decision.reason


def test_decisions_are_clamped_and_deterministic():
    trace = open_loop_trace(SYNTHETIC["overload-recovery"])
    first = [item.to_dict() for item in replay(trace)]
    second = [item.to_dict() for item in replay(trace)]
    assert first == second
    assert {item["replicas"] for item in first} <= {1, 2, 3}
    narrow = PlannerConfig(max_replicas=2)
    assert max(item.replicas for item in replay(trace, config=narrow)) == 2


def test_planner_config_rejects_bounds_beyond_partitions():
    with pytest.raises(ValueError, match="Replica bounds"):
        PlannerConfig(max_replicas=4)
    with pytest.raises(ValueError, match="positive"):
        PlannerConfig(capacity_per_consumer=0)


def test_unknown_policy_is_rejected():
    with pytest.raises(ValueError, match="Unknown policy"):
        build_policy("oracle", CONFIG)


def test_planner_holds_while_scale_up_is_in_flight():
    planner = Planner(CONFIG)
    surge = [390] * CONFIG.horizon_seconds
    first = planner.decide(observation(0, 0, lag=300, ready=1, desired=1), surge)
    assert first.replicas == 3
    pending = planner.decide(observation(2, 780, lag=600, ready=1, desired=3), surge)
    assert pending.replicas == 3
    assert not pending.changed


def test_replay_command_prints_proposals(tmp_path, capsys, monkeypatch):
    from streamscale import cli

    output = tmp_path / "decisions.jsonl"
    monkeypatch.setattr(
        "sys.argv",
        ["streamscale", "autoscale-replay", "--synthetic", "flash-sale", "--output", str(output)],
    )
    assert cli.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["max_replicas"] == 3
    assert [item["replicas"] for item in result["proposals"]][:2] == [1, 3]
    assert len(output.read_text().splitlines()) == result["steps"]


def exporter_text(run_id, highs, lags, success):
    lines = []
    for part in range(3):
        labels = f'run_id="{run_id}",mode="ml-reactive",scenario="flash-sale",partition="{part}"'
        lines.append(f"streamscale_kafka_lag{{{labels}}} {lags[part]}")
        lines.append(f"streamscale_kafka_high_watermark{{{labels}}} {highs[part]}")
    lines.append(
        f'streamscale_exporter_last_success_unixtime{{run_id="{run_id}",mode="ml-reactive",'
        f'scenario="flash-sale"}} {success}'
    )
    return "\n".join(lines) + "\n"


class FakeScaler:
    def __init__(self):
        self.desired, self.ready, self.calls = 1, 1, []

    def status(self):
        return self.desired, self.ready

    def scale(self, replicas):
        self.calls.append(replicas)
        self.desired = replicas


def test_controller_scales_through_the_deployment_and_records_decisions():
    scaler = FakeScaler()
    controller = Controller("reactive", CONFIG, scaler, "http://exporter/metrics")
    high = 0
    for second in range(4):
        high += 390 if second >= 2 else 52
        payload = exporter_text(
            "run-a", (high // 3,) * 3, (0, 0, 0) if second < 2 else (90,) * 3, second
        )
        record = controller.tick(now=float(second), fetch=lambda _url, text=payload: text)
    assert scaler.calls == [3]
    assert record["run_id"] == "run-a"
    assert any(item["applied"] for item in controller.recent())


def test_controller_resets_its_history_for_a_new_run():
    scaler = FakeScaler()
    controller = Controller("reactive", CONFIG, scaler, "http://exporter/metrics")
    controller.tick(
        now=0.0, fetch=lambda _url: exporter_text("run-a", (900, 900, 900), (0,) * 3, 0)
    )
    first = controller.policy
    # A new run's offsets may be lower than the old ones; that must not look like a rewind.
    controller.tick(now=1.0, fetch=lambda _url: exporter_text("run-b", (10, 10, 10), (0,) * 3, 1))
    assert controller.policy is not first
    assert controller.run_id == "run-b"


def test_scaler_refuses_replicas_outside_the_contract(tmp_path, monkeypatch):
    for name, value in (("namespace", "streamscale"), ("token", "t"), ("ca.crt", "")):
        (tmp_path / name).write_text(value)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setattr("ssl.create_default_context", lambda **_: None)
    scaler = DeploymentScaler("streamscale-consumer", account=tmp_path)
    assert scaler.base.endswith("/namespaces/streamscale/deployments/streamscale-consumer")
    for invalid in (0, 4, 2.0):
        with pytest.raises(ValueError, match="1 to 3"):
            scaler.scale(invalid)


def test_existing_backlog_without_new_arrivals_still_scales_out():
    # Regression from the first kind run: an unpaced publish left 5,700 records queued with
    # zero arrivals; delay before any pod could start masked every candidate equally.
    policy = ReactivePolicy(CONFIG)
    policy.step(observation(0, 5940, lag=5716))
    decision = policy.step(observation(1, 5940, lag=5610))
    assert decision.replicas == 3
    assert decision.predicted_delay[3] < decision.predicted_delay[1]


def test_forecast_only_scale_up_needs_confirmation_but_current_load_does_not():
    planner = Planner(PlannerConfig(forecast_confirmations=2))
    quiet = [52] * CONFIG.horizon_seconds
    rush = [390] * CONFIG.horizon_seconds
    first = planner.decide(observation(0, 0), rush, current=quiet)
    assert first.replicas == 1 and "confirming" in first.reason
    second = planner.decide(observation(1, 52), rush, current=quiet)
    assert second.replicas == 3 and second.reason.startswith("forecast:")
    # When today's rate already needs more consumers, act at once.
    urgent = Planner(PlannerConfig(forecast_confirmations=2))
    assert urgent.decide(observation(0, 0, lag=300), rush, current=rush).replicas == 3


def test_planner_uses_capacity_per_consumer_count():
    slow = PlannerConfig(capacity_scale=(1.0, 0.8, 0.7))
    rates = [330] * CONFIG.horizon_seconds
    assert simulate((0, 0, 0), rates, 3, 3, 0, CONFIG) < 1
    assert simulate((0, 0, 0), rates, 3, 3, 0, slow) > CONFIG.target_delay_seconds
    with pytest.raises(ValueError, match="three positive"):
        PlannerConfig(capacity_scale=(1.0, 0.0, 1.0))

"""Scaling policies: arrival estimate or forecast, fed to the shared planner."""

from collections import deque
from dataclasses import replace

from streamscale.autoscale.features import ArrivalHistory
from streamscale.autoscale.observation import Observation
from streamscale.autoscale.planner import Decision, Planner, PlannerConfig

POLICIES = ("reactive", "predictive")
# Follow-up ablations of ml-predictive; "full" is the frozen M16 behaviour.
VARIANTS = ("full", "forecast-only", "adapt-only")


class ReactivePolicy:
    """Ablation baseline: assumes the recent arrival rate continues; nothing is learned.

    It shares the 1 s loop, planner and actuation with the predictive policy, so the
    difference between the two isolates what forecasting and online learning add.
    """

    name = "reactive"

    def __init__(self, config: PlannerConfig, rate_window: float = 3.0):
        self.config = config
        self.rate_window = rate_window
        self.history = ArrivalHistory(config.capacity_per_consumer)
        self.planner = Planner(config)

    def step(self, observation: Observation) -> Decision:
        self.history.add(observation)
        rate = self.arrival_rate()
        if rate is None:
            return self.planner.hold(observation, "waiting for a second offset sample")
        horizon = [rate] * self.config.horizon_seconds
        return self.planner.decide(observation, horizon, rate=rate, forecast=rate)

    def arrival_rate(self):
        """Fast-attack, slow-decay estimate: rises with the last second, falls over 3 s."""
        recent = self.history.rate(1)
        smoothed = self.history.rate(self.rate_window)
        if recent is None or smoothed is None:
            return None
        return max(recent, smoothed)

    def describe(self):
        return {"policy": self.name, "planner": self.config.to_dict()}


class Bounded:
    """Exponentially weighted estimate clamped to [low, high]."""

    def __init__(self, initial, low, high, weight):
        self.value = initial
        self.low, self.high, self.weight = low, high, weight
        self.samples = 0

    def update(self, sample):
        clipped = min(self.high, max(self.low, sample))
        self.value = min(
            self.high, max(self.low, self.value + self.weight * (clipped - self.value))
        )
        self.samples += 1
        return self.value


class PredictivePolicy(ReactivePolicy):
    """Quantile forecast of peak arrivals, plus bounded online adaptation.

    Online adaptation learns three things from the running system, each kept within bounds:
    a forecast bias (recent under-forecast of the next window), per-consumer capacity
    (measured while every partition has a backlog) and the pod start-to-Ready delay.
    No forecast window falls below the reactive estimate, so the forecast can only add
    consumers earlier than the ablation would, never remove them sooner. Learned capacity and
    pod delay can still change decisions in either direction.
    """

    name = "predictive"

    def __init__(
        self, config: PlannerConfig, forecaster, rate_window: float = 3.0, variant: str = "full"
    ):
        if variant not in VARIANTS:
            raise ValueError(f"Unknown variant {variant!r}; expected one of {', '.join(VARIANTS)}")
        # Ablation switches (M16 follow-up). "full" is the frozen ml-predictive behaviour.
        self.variant = variant
        self.use_forecast = variant in {"full", "forecast-only"}
        self.use_adaptation = variant in {"full", "adapt-only"}
        # A leading quiet second that only the forecast flags waits for confirmation.
        config = replace(config, forecast_confirmations=max(config.forecast_confirmations, 2))
        super().__init__(config, rate_window)
        self.forecaster = forecaster
        self.pilot = config.capacity_per_consumer
        self.bias = Bounded(0.0, -0.5 * self.pilot, 0.5 * self.pilot, 0.1)
        # Learned separately per consumer count: on one shared node, three consumers each
        # run slower than one alone, so a single estimate would mislead every candidate.
        self.capacities = {
            count: Bounded(self.pilot, 0.6 * self.pilot, 1.4 * self.pilot, 0.1)
            for count in (1, 2, 3)
        }
        self.ready_delay = Bounded(config.ready_delay_seconds, 1.0, 20.0, 0.3)
        self.pending: deque[tuple[float, float]] = deque()  # (made_at, first-window forecast)
        self.requested: deque[tuple[float, int]] = deque()  # (requested_at, target count)
        self.previous: Observation | None = None
        self.first_window = forecaster.horizons[0]

    def learn(self, observation: Observation):
        start, end = self.first_window
        while self.pending and self.pending[0][0] + end + 1 <= observation.at:
            made_at, predicted = self.pending.popleft()
            realized = self.history.peak_rate(made_at + start, made_at + end + 1)
            if realized is not None:
                self.bias.update(realized - predicted)
        previous = self.previous
        if previous is not None and observation.at > previous.at:
            elapsed = observation.at - previous.at
            steady = observation.ready == previous.ready and observation.ready > 0
            busy = min(observation.lags) > 0.5 * self.pilot and min(previous.lags) > 0
            if steady and busy:
                served = (observation.high - previous.high) - (observation.lag - previous.lag)
                count = min(observation.ready, 3)
                self.capacities[count].update(served / elapsed / count)
        while self.requested and observation.ready >= self.requested[0][1]:
            requested_at, _ = self.requested.popleft()
            self.ready_delay.update(observation.at - requested_at)
        self.previous = observation

    def step(self, observation: Observation) -> Decision:
        self.history.add(observation)
        self.learn(observation)
        rate = self.arrival_rate()
        features = self.history.features()
        if rate is None or features is None:
            return self.planner.hold(observation, "waiting for a second offset sample")
        if self.use_adaptation:
            capacity = self.capacities[1].value
            self.planner.config = replace(
                self.config,
                capacity_per_consumer=capacity,
                capacity_scale=tuple(
                    self.capacities[count].value / capacity for count in (1, 2, 3)
                ),
                ready_delay_seconds=self.ready_delay.value,
            )
        # The model is always evaluated (same work and startup in every variant); the
        # ablations only change whether its output and the learned bias are used.
        windows = self.forecaster.predict(features, self.pilot)
        bias = self.bias.value if self.use_adaptation else 0.0
        if self.use_forecast:
            adjusted = [max(rate, value + bias) for value in windows]
        else:
            adjusted = [rate] * len(windows)
        self.pending.append((observation.at, windows[0]))
        horizon = []
        for (start, end), value in zip(self.forecaster.horizons, adjusted, strict=True):
            horizon.extend([value] * (end - start + 1))
        decision = self.planner.decide(
            observation,
            horizon,
            rate=rate,
            forecast=max(adjusted),
            current=[rate] * self.config.horizon_seconds,
        )
        if decision.replicas > observation.desired:
            self.requested.append((observation.at, decision.replicas))
        elif decision.replicas < observation.desired:
            self.requested.clear()
        decision.learned = self.learned()
        return decision

    def learned(self):
        return {
            "bias": round(self.bias.value, 2),
            "capacity": round(self.capacities[1].value, 2),
            "capacity_by_replicas": {
                str(count): round(estimate.value, 2) for count, estimate in self.capacities.items()
            },
            "ready_delay": round(self.ready_delay.value, 2),
        }

    def describe(self):
        return {
            "policy": self.name,
            "variant": self.variant,
            "planner": self.config.to_dict(),
            "model_sha256": self.forecaster.sha256,
            "learned": self.learned(),
        }


def build_policy(name: str, config: PlannerConfig, forecaster=None, variant: str = "full"):
    if name == "reactive":
        if variant != "full":
            raise ValueError("Variants apply to the predictive policy only")
        return ReactivePolicy(config)
    if name == "predictive":
        if forecaster is None:
            from streamscale.autoscale.forecast import load

            forecaster = load()
        return PredictivePolicy(config, forecaster, variant=variant)
    raise ValueError(f"Unknown policy {name!r}; expected one of {', '.join(POLICIES)}")

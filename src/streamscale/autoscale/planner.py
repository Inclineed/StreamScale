"""Model-predictive replica planner over a per-partition queue model.

For each candidate replica count the planner simulates the next ``horizon_seconds`` of
backlog, including pod start and the consumer-group rebalance pause, and chooses the fewest
consumers whose predicted queueing delay stays within the target. Scale-up is immediate;
scale-down waits until a lower count has qualified for a hold period.
"""

import math
from dataclasses import asdict, dataclass, field

from streamscale.autoscale.observation import PARTITIONS, Observation

# librdkafka's default range assignor on three partitions.
ASSIGNMENTS = {
    1: ((0, 1, 2),),
    2: ((0, 1), (2,)),
    3: ((0,), (1,), (2,)),
}
# When no count meets the target, more consumers must cut predicted delay by more than this.
MATERIAL_SECONDS = 0.5


@dataclass(frozen=True)
class PlannerConfig:
    min_replicas: int = 1
    max_replicas: int = 3
    target_delay_seconds: float = 3.0
    horizon_seconds: int = 20
    capacity_per_consumer: float = 130.0
    ready_delay_seconds: float = 5.0
    rebalance_pause_seconds: float = 1.0
    scale_down_hold_seconds: float = 10.0
    scale_down_interval_seconds: float = 20.0
    stale_after_seconds: float = 5.0
    # Per-consumer capacity multipliers when 1, 2 or 3 consumers share the node.
    capacity_scale: tuple[float, float, float] = (1.0, 1.0, 1.0)
    # A scale-up that only the forecast (not the current rate) needs must persist this
    # many consecutive decisions, so a rebalance blip cannot trigger a flap.
    forecast_confirmations: int = 2

    def capacity_at(self, replicas):
        if replicas <= 0:
            return self.capacity_per_consumer
        return self.capacity_per_consumer * self.capacity_scale[min(replicas, PARTITIONS) - 1]

    def __post_init__(self):
        if len(self.capacity_scale) != PARTITIONS or min(self.capacity_scale) <= 0:
            raise ValueError("Capacity scale needs three positive multipliers")
        if self.forecast_confirmations < 1:
            raise ValueError("Forecast confirmations must be at least 1")
        if not 1 <= self.min_replicas <= self.max_replicas <= PARTITIONS:
            raise ValueError(f"Replica bounds must satisfy 1 <= min <= max <= {PARTITIONS}")
        positive = (
            self.target_delay_seconds,
            self.capacity_per_consumer,
            self.horizon_seconds,
        )
        if min(positive) <= 0:
            raise ValueError("Target delay, capacity and horizon must be positive")
        nonnegative = (
            self.ready_delay_seconds,
            self.rebalance_pause_seconds,
            self.scale_down_hold_seconds,
            self.scale_down_interval_seconds,
            self.stale_after_seconds,
        )
        if min(nonnegative) < 0:
            raise ValueError("Delays and hold times must be nonnegative")

    def to_dict(self):
        result = asdict(self)
        result["capacity_scale"] = list(self.capacity_scale)
        return result


@dataclass
class Decision:
    at: float
    replicas: int
    previous: int
    reason: str
    arrival_rate: float | None = None
    forecast_rate: float | None = None
    predicted_delay: dict[int, float] = field(default_factory=dict)
    learned: dict[str, float] | None = None

    @property
    def changed(self):
        return self.replicas != self.previous

    def to_dict(self):
        result = asdict(self)
        result["predicted_delay"] = {
            str(key): round(value, 3) for key, value in self.predicted_delay.items()
        }
        return result


def controllable_from(config: PlannerConfig):
    """First horizon step any decision made now can influence (pod start plus rebalance).

    Delay before this step is the same for every candidate, so including it would let an
    existing backlog mask the difference between counts.
    """
    return min(
        config.horizon_seconds - 1,
        math.ceil(config.ready_delay_seconds + config.rebalance_pause_seconds),
    )


def simulate(
    lags, rates, ready, candidate, switch_after, config: PlannerConfig, later=(), from_step=0
):
    """Worst predicted queueing delay (seconds) from ``from_step`` to the horizon's end.

    ``rates`` holds total arrivals per second for each horizon step, split evenly across
    partitions (records are keyed by order id). Until ``switch_after`` seconds the current
    ``ready`` consumers serve; then service stops for the rebalance pause and ``candidate``
    consumers take over. ``later`` lists further ``(at, count)`` switches, used to model a
    future scale-up decision. A consumer serves its partitions work-conservingly.
    """
    queues = [float(value) for value in lags]
    switches = [(switch_after, candidate)] if candidate != ready else []
    switches += sorted(later)
    worst = 0.0
    active_count = ready
    index = 0
    resume = 0.0
    for step, rate in enumerate(rates):
        start = float(step)
        for part in range(PARTITIONS):
            queues[part] += rate / PARTITIONS
        while index < len(switches) and start >= switches[index][0]:
            at, count = switches[index]
            if count != active_count:
                # A first join pauses nobody; later membership changes pause every member.
                pause = config.rebalance_pause_seconds if active_count > 0 else 0.0
                resume = at + pause
                active_count = count
            index += 1
        active = active_count if start >= resume else 0
        capacity = config.capacity_at(active if active > 0 else max(active_count, 1))
        if active > 0:
            for group in ASSIGNMENTS[active]:
                total = sum(queues[part] for part in group)
                if total <= 0:
                    continue
                served = min(total, capacity)
                for part in group:
                    queues[part] -= served * queues[part] / total
        if step < from_step:
            continue
        groups = ASSIGNMENTS[active if active > 0 else max(active_count, 1)]
        waiting = max(0.0, resume - start - 1) if active == 0 else 0.0
        for group in groups:
            worst = max(worst, sum(queues[part] for part in group) / capacity + waiting)
    return worst


class Planner:
    """Stateful wrapper: tracks recent changes so holds and pod-start delays apply."""

    def __init__(self, config: PlannerConfig):
        self.config = config
        self.first_seen: float | None = None
        self.last_change_at: float | None = None
        self.last_scale_up_at: float | None = None
        self.qualified_since: dict[int, float] = {}
        self.forecast_streak = 0

    def remaining_start(self, now, ready, desired, candidate):
        """Seconds until ``candidate`` consumers could be serving.

        Pods already requested (up to ``desired``) count from this planner's last scale-up,
        or from its first observation when they predate it, such as a fresh group's first pod.
        """
        if candidate <= ready:
            return 0.0
        if candidate <= desired:
            started = (
                self.last_scale_up_at if self.last_scale_up_at is not None else self.first_seen
            )
            if started is not None:
                return max(0.0, self.config.ready_delay_seconds - (now - started))
        return self.config.ready_delay_seconds

    def hold(self, observation: Observation, reason, rate=None, forecast=None):
        if self.first_seen is None:
            self.first_seen = observation.at
        current = self.clamp(observation.desired)
        return Decision(observation.at, current, observation.desired, reason, rate, forecast)

    def clamp(self, replicas):
        return max(self.config.min_replicas, min(self.config.max_replicas, replicas))

    def stale(self, observation: Observation):
        if observation.fresh_at is None:
            return False
        return observation.at - observation.fresh_at > self.config.stale_after_seconds

    def delays_for(self, observation, rates, ready):
        """(rescue-aware delays, committed delays) for every candidate count."""
        config = self.config
        # Receding horizon with recourse: a smaller count now is judged assuming the next
        # decision (one second later) can still add consumers, which then need the full pod
        # start delay. Only load that arrives before that rescue could take effect forces a
        # scale-up now, so distant uncertain peaks do not keep idle consumers running.
        rescue = [(1.0 + config.ready_delay_seconds, config.max_replicas)]
        window = controllable_from(config)
        delays, committed = {}, {}
        for candidate in range(config.min_replicas, config.max_replicas + 1):
            start = self.remaining_start(observation.at, ready, observation.desired, candidate)
            committed[candidate] = simulate(
                observation.lags, rates, ready, candidate, start, config, from_step=window
            )
            delays[candidate] = (
                simulate(
                    observation.lags,
                    rates,
                    ready,
                    candidate,
                    start,
                    config,
                    rescue,
                    from_step=window,
                )
                if candidate < config.max_replicas
                else committed[candidate]
            )
        return delays, committed

    def horizon(self, rates):
        rates = list(rates)[: self.config.horizon_seconds]
        if len(rates) < self.config.horizon_seconds:
            rates += [rates[-1] if rates else 0.0] * (self.config.horizon_seconds - len(rates))
        return rates

    def decide(self, observation: Observation, rates, rate=None, forecast=None, current=None):
        """Choose a replica count given a per-second arrival forecast over the horizon.

        ``current`` is the arrival path that assumes today's rate continues. When given and
        it differs from ``rates``, scale-ups needed only by the forecast must be confirmed
        on consecutive decisions; scale-ups the current path already needs are immediate.
        """
        config = self.config
        now = observation.at
        if self.first_seen is None:
            self.first_seen = now
        if self.stale(observation):
            return self.hold(observation, "stale exporter sample; holding", rate, forecast)
        rates = self.horizon(rates)
        baseline = None if current is None else self.horizon(current)
        current = self.clamp(observation.desired)
        ready = min(observation.ready, PARTITIONS)
        delays, committed = self.delays_for(observation, rates, ready)
        qualified = [n for n, delay in delays.items() if delay <= config.target_delay_seconds]
        if qualified:
            wanted = qualified[0]
        else:
            # Nothing meets the target even with a later rescue: act now, choosing the least
            # committed delay; extra consumers must buy a material improvement.
            delays = committed
            best = min(delays.values())
            wanted = min(n for n, delay in delays.items() if delay <= best + MATERIAL_SECONDS)
        for candidate in list(self.qualified_since):
            if candidate not in qualified:
                del self.qualified_since[candidate]
        for candidate in qualified:
            self.qualified_since.setdefault(candidate, now)

        forecast_only = False
        if wanted > current and baseline is not None and baseline != rates:
            now_delays, _ = self.delays_for(observation, baseline, ready)
            forecast_only = now_delays[current] <= config.target_delay_seconds
        if wanted > current and forecast_only:
            self.forecast_streak += 1
        else:
            self.forecast_streak = 0

        if (
            wanted > current
            and forecast_only
            and (self.forecast_streak < config.forecast_confirmations)
        ):
            chosen, reason = (
                current,
                f"forecast wants {wanted}; confirming "
                f"({self.forecast_streak}/{config.forecast_confirmations})",
            )
        elif wanted > current:
            reason = (
                f"predicted delay {delays[current]:.1f}s at {current} exceeds "
                f"{config.target_delay_seconds:g}s target; {wanted} meets it"
                if qualified
                else f"no count meets the {config.target_delay_seconds:g}s target; "
                f"{wanted} minimises predicted delay ({delays[wanted]:.1f}s)"
            )
            if forecast_only:
                reason = "forecast: " + reason
            chosen = wanted
        elif wanted < current:
            held = now - self.qualified_since.get(wanted, now)
            since_change = (
                float("inf") if self.last_change_at is None else now - self.last_change_at
            )
            if held < config.scale_down_hold_seconds:
                chosen, reason = (
                    current,
                    (
                        f"{wanted} meets the target; holding {current} until it has for "
                        f"{config.scale_down_hold_seconds:g}s"
                    ),
                )
            elif since_change < config.scale_down_interval_seconds:
                chosen, reason = (
                    current,
                    (f"{wanted} meets the target; last change was {since_change:.0f}s ago"),
                )
            else:
                chosen, reason = (
                    wanted,
                    (
                        f"{wanted} has met the {config.target_delay_seconds:g}s target for "
                        f"{held:.0f}s; scaling in"
                    ),
                )
        else:
            chosen, reason = current, f"{current} meets the {config.target_delay_seconds:g}s target"
            if not qualified:
                reason = (
                    f"no count meets the {config.target_delay_seconds:g}s target; "
                    f"{current} minimises predicted delay ({delays[current]:.1f}s)"
                )

        if chosen != observation.desired:
            self.last_change_at = now
            if chosen > observation.desired:
                self.last_scale_up_at = now
        return Decision(now, chosen, observation.desired, reason, rate, forecast, delays)

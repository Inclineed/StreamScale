"""Closed-loop, one-second queue simulator of the StreamScale consumer group.

The model follows the measured pipeline: three partitions split evenly by order key, one
sequential consumer per pod serving its partitions work-conservingly, pods that take a
fixed delay to become ready, an eager rebalance pause on every membership change, and the
correctness profile's 2% extra records and 5% of records delayed by 10 s.

Controllers see only what the real ones see: lag, high watermarks, ready/desired replicas
and, for the CPU baseline, delayed CPU samples. Parameters are calibrated against M7.
"""

import math
from collections import deque
from dataclasses import asdict, dataclass, field, replace

from streamscale.autoscale.observation import PARTITIONS, Observation
from streamscale.autoscale.planner import ASSIGNMENTS


@dataclass(frozen=True)
class Cluster:
    capacity: float = 132.0  # records/s for one consumer
    efficiency_3: float = 1.0  # per-consumer capacity multiplier at three consumers
    ready_delay: float = 4.5  # pod creation to serving records, including group join
    probe_lag: float = 0.0  # serving to the Deployment reporting Ready (3 s readiness probe)
    rebalance_pause: float = 1.0  # all members pause on a membership change
    startup_seconds: float = 2.0  # first consumer of a fresh group
    base_latency: float = 0.6  # batching latency when nothing is queued
    extra_fraction: float = 0.02  # duplicates added by the correctness profile
    late_fraction: float = 0.05
    late_seconds: int = 10
    hpa_phase: float = 2.0  # seconds from publication start to the first HPA sync
    cpu_base: float = 0.01  # cores per idle consumer
    cpu_per_record: float = 0.00085  # cores per record/s

    def capacity_at(self, replicas):
        if replicas >= 3:
            return self.capacity * self.efficiency_3
        if replicas == 2:
            return self.capacity * (1 + self.efficiency_3) / 2
        return self.capacity

    def to_dict(self):
        return asdict(self)


def arrivals(schedule, cluster: Cluster):
    """Records reaching Kafka in each second, including the correctness profile."""
    base = [rate for seconds, rate in schedule for _ in range(seconds)]
    total = [0.0] * (len(base) + cluster.late_seconds)
    scale = 1 + cluster.extra_fraction
    for second, rate in enumerate(base):
        total[second] += rate * (1 - cluster.late_fraction) * scale
        total[second + cluster.late_seconds] += rate * cluster.late_fraction * scale
    return total


@dataclass
class View:
    """What a controller may read at the start of a second."""

    at: float
    highs: tuple[int, ...]
    lags: tuple[int, ...]
    ready: int
    desired: int
    cpu_utilization: float | None  # percent of the 100m request, delayed like metrics-server

    def observation(self):
        return Observation(self.at, self.highs, self.lags, self.ready, self.desired, self.at)


class FixedController:
    name = "fixed"

    def start(self, cluster, seed):
        pass

    def step(self, view):
        return 1


class HpaLike:
    """autoscaling/v2 behaviour used by both measured autoscalers (15 s sync, +2/-1 pods)."""

    sync = 15
    tolerance = 0.1

    def __init__(self, maximum=3, scale_down_seconds=60):
        self.maximum = maximum
        self.scale_down_seconds = scale_down_seconds

    def start(self, cluster, seed):
        self.next_sync = cluster.hpa_phase
        self.recommendations = deque()
        self.desired = 1

    def raw(self, view):  # pragma: no cover - implemented by subclasses
        raise NotImplementedError

    def step(self, view):
        if view.at + 1e-9 < self.next_sync:
            return self.desired
        self.next_sync += self.sync
        proposed = self.raw(view)
        if proposed is None:
            return self.desired
        proposed = max(1, min(self.maximum, proposed))
        self.recommendations.append((view.at, proposed))
        while self.recommendations[0][0] < view.at - self.scale_down_seconds:
            self.recommendations.popleft()
        if proposed > self.desired:
            self.desired = min(proposed, self.desired + 2)
        else:
            floor = max(value for _, value in self.recommendations)
            if floor < self.desired:
                self.desired = max(floor, self.desired - 1)
        return self.desired


class KedaLike(HpaLike):
    """Lag trigger: KEDA polls every 2 s and the HPA divides total lag by 100 per replica."""

    name = "lag-keda"
    poll = 2

    def start(self, cluster, seed):
        super().start(cluster, seed)
        self.cached = 0
        self.next_poll = 0.0

    def step(self, view):
        if view.at + 1e-9 >= self.next_poll:
            self.cached = sum(view.lags)
            self.next_poll += self.poll
        return super().step(view)

    def raw(self, view):
        current = max(self.desired, 1)
        ratio = self.cached / (100 * current)
        if abs(ratio - 1) <= self.tolerance:
            return self.desired
        return max(1, math.ceil(self.cached / 100))


class CpuHpaLike(HpaLike):
    name = "cpu-hpa"

    def raw(self, view):
        if view.cpu_utilization is None:
            return None
        ratio = view.cpu_utilization / 60
        if abs(ratio - 1) <= self.tolerance:
            return self.desired
        return math.ceil(view.ready * ratio) if view.ready else None


class PolicyController:
    """Adapter for streamscale.autoscale policies (ml-reactive, ml-predictive)."""

    def __init__(self, factory, name):
        self.factory = factory
        self.name = name

    def start(self, cluster, seed):
        self.policy = self.factory()
        self.decisions = []

    def step(self, view):
        decision = self.policy.step(view.observation())
        self.decisions.append(decision)
        return decision.replicas


@dataclass
class Result:
    p95_latency: float
    peak_lag: float
    replica_seconds: float
    drain_seconds: float
    max_ready: int
    scale_changes: int
    ready_at_sale_start: list[int] = field(default_factory=list)
    scaling_response: float | None = None
    lag: list[float] = field(default_factory=list)
    ready: list[int] = field(default_factory=list)
    arrivals: list[float] = field(default_factory=list)

    def summary(self):
        value = asdict(self)
        for key in ("lag", "ready", "arrivals"):
            value.pop(key)
        return value


def percentile(weighted, quantile):
    """Nearest-rank percentile of (value, weight) pairs."""
    total = sum(weight for _, weight in weighted)
    if total <= 0:
        return 0.0
    rank = quantile * total
    seen = 0.0
    for value, weight in sorted(weighted):
        seen += weight
        if seen >= rank - 1e-9:
            return value
    return max(value for value, _ in weighted)


def run(
    schedule,
    controller,
    cluster: Cluster,
    sale_starts=(),
    seed=0,
    max_seconds=900,
):
    """Simulate one execution; returns outcome metrics comparable to the measured runner."""
    incoming = arrivals(schedule, cluster)
    publication_end = len(incoming)
    controller.start(cluster, seed)
    queues = [deque() for _ in range(PARTITIONS)]  # cohorts: [arrival_second, records]
    highs = [0.0] * PARTITIONS
    pods = []  # ready_at times; a pod counts as ready once now >= ready_at
    pause_until = 0.0
    desired = 1
    pods.append(cluster.startup_seconds)
    previous_ready = 0
    cpu_window = deque()
    reported_cpu = None
    latencies = []
    result = Result(0, 0, 0, 0, 0, 0)
    drained_at = None
    changes = 0
    reported = []
    for second in range(max_seconds):
        now = float(second)
        ready = min(PARTITIONS, sum(1 for ready_at in pods if ready_at <= now))
        visible = min(
            PARTITIONS, sum(1 for ready_at in pods if ready_at + cluster.probe_lag <= now)
        )
        if ready != previous_ready:
            if previous_ready > 0:
                pause_until = max(pause_until, now + cluster.rebalance_pause)
            previous_ready = ready
        if second % 10 == 0 and cpu_window:
            reported_cpu = sum(cpu_window) / len(cpu_window)
        lags = tuple(int(round(sum(item[1] for item in queue))) for queue in queues)
        view = View(
            now,
            tuple(int(value) for value in highs),
            lags,
            visible,
            desired,
            reported_cpu,
        )
        wanted = max(1, min(PARTITIONS, int(controller.step(view))))
        if wanted != desired:
            changes += 1
            if wanted > desired:
                pods.extend([now + cluster.ready_delay] * (wanted - desired))
            else:
                # Pods leave at once; the newest (possibly not yet ready) go first.
                pods = sorted(pods)[:wanted]
                ready_now = min(PARTITIONS, sum(1 for ready_at in pods if ready_at <= now))
                if ready_now != previous_ready:
                    pause_until = max(pause_until, now + cluster.rebalance_pause)
                    previous_ready = ready_now
                ready = ready_now
                visible = min(visible, len(pods))
            desired = wanted
        if second in sale_starts:
            result.ready_at_sale_start.append(visible)
        reported.append(visible)

        rate = incoming[second] if second < publication_end else 0.0
        for part in range(PARTITIONS):
            share = rate / PARTITIONS
            if share > 0:
                queues[part].append([now, share])
                highs[part] += share
        available = max(0.0, 1.0 - max(0.0, pause_until - now))
        served_total = 0.0
        if ready > 0 and available > 0:
            capacity = cluster.capacity_at(ready) * available
            for group in ASSIGNMENTS[ready]:
                pending = [sum(item[1] for item in queues[part]) for part in group]
                total = sum(pending)
                if total <= 0:
                    continue
                budget = min(total, capacity)
                for part, amount in zip(group, pending, strict=True):
                    take = budget * amount / total
                    served_total += take
                    queue = queues[part]
                    while take > 1e-9 and queue:
                        cohort = queue[0]
                        used = min(take, cohort[1])
                        wait = max(0.0, now - cohort[0])
                        latencies.append((wait + cluster.base_latency, used))
                        cohort[1] -= used
                        take -= used
                        if cohort[1] <= 1e-9:
                            queue.popleft()
        per_pod = served_total / ready if ready else 0.0
        cpu_window.append(100 * (cluster.cpu_base + cluster.cpu_per_record * per_pod) / 0.1)
        if len(cpu_window) > 10:
            cpu_window.popleft()
        backlog = sum(sum(item[1] for item in queue) for queue in queues)
        result.lag.append(backlog)
        result.ready.append(ready)
        result.arrivals.append(rate)
        if second >= publication_end - 1 and backlog < 0.5 and drained_at is None:
            drained_at = second + 1
            break
    if drained_at is None:
        raise RuntimeError("Simulated backlog did not drain")
    result.p95_latency = percentile(latencies, 0.95)
    result.peak_lag = max(result.lag)
    # Like the measured runner: Ready pods, and the first rise after the load transition.
    result.ready = reported
    result.replica_seconds = float(sum(reported[:drained_at]))
    transition = sale_starts[0] if sale_starts else 0
    baseline = reported[transition] if transition < len(reported) else 0
    if transition == 0:
        baseline = max(1, baseline)
    rises = [
        second - transition
        for second in range(transition, len(reported))
        if reported[second] > baseline
    ]
    result.scaling_response = float(rises[0]) if rises else None
    result.drain_seconds = float(drained_at)
    result.max_ready = max(result.ready)
    result.scale_changes = changes
    return result


def with_parameters(cluster: Cluster, **values):
    return replace(cluster, **values)

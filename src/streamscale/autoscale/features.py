"""Arrival history and forecaster features, shared by training and live control.

Rates are expressed in units of one consumer's capacity, so a model trained against one pilot
calibration still applies when the frozen capacity differs.
"""

from collections import deque

from streamscale.autoscale.observation import Observation

# Feature order is part of the model contract; forecaster JSON files record it.
FEATURES = (
    "rate_1",
    "rate_5",
    "rate_15",
    "rate_60",
    "slope_5",
    "lag",
    "in_sale",
    "since_sale_start",
    "since_sale_end",
    "last_sale_seconds",
    "last_gap_seconds",
    "sales_seen",
)
# Deliberately absent: time since the run started. Every v3 run's first sale begins at a fixed
# offset, so that feature would let a model memorise the protocol instead of the traffic.
# Placeholder for "has not happened yet"; trees split on it cleanly.
NEVER = 999.0


class ArrivalHistory:
    """Keeps recent (time, summed high watermark) points and detects sale periods.

    A sale is a period when arrivals exceed ``sale_threshold`` consumer capacities, i.e. more
    than one consumer can serve. Detection uses only observed offsets.
    """

    def __init__(self, capacity_per_consumer: float, sale_threshold=1.2, keep_seconds=120):
        if capacity_per_consumer <= 0:
            raise ValueError("Consumer capacity must be positive")
        self.capacity = float(capacity_per_consumer)
        self.sale_threshold = sale_threshold
        self.keep = keep_seconds
        self.points: deque[tuple[float, int]] = deque()
        self.lag = 0
        self.started_at: float | None = None
        self.in_sale = False
        self.sale_starts: list[float] = []
        self.sale_ends: list[float] = []

    def add(self, observation: Observation):
        if self.points and observation.at <= self.points[-1][0]:
            return  # Out-of-order or duplicate sample; keep the history monotonic.
        if self.points and observation.high < self.points[-1][1]:
            raise ValueError("High watermarks moved backwards; the topic changed")
        if self.started_at is None:
            self.started_at = observation.at
        self.points.append((observation.at, observation.high))
        self.lag = observation.lag
        while self.points and self.points[0][0] < observation.at - self.keep:
            self.points.popleft()
        rate = self.rate(3)
        if rate is None:
            return
        busy = rate > self.sale_threshold * self.capacity
        if busy and not self.in_sale:
            self.sale_starts.append(observation.at)
        elif not busy and self.in_sale:
            self.sale_ends.append(observation.at)
        self.in_sale = busy

    @property
    def now(self):
        return self.points[-1][0] if self.points else None

    def rate(self, window: float, ending: float | None = None):
        """Average arrivals per second over the most recent ``window`` seconds."""
        if len(self.points) < 2:
            return None
        end = self.now if ending is None else ending
        last = self._at_or_before(end)
        first = self._at_or_before(end - window)
        if last is None:
            return None
        if first is None:
            first = self.points[0]
        elapsed = last[0] - first[0]
        if elapsed <= 0:
            return None
        return (last[1] - first[1]) / elapsed

    def _at_or_before(self, moment):
        found = None
        for point in self.points:
            if point[0] > moment + 1e-9:
                break
            found = point
        return found

    def peak_rate(self, start: float, end: float):
        """Highest one-second arrival rate in (start, end], or None if not yet observed."""
        best = None
        previous = None
        for point in self.points:
            if previous is not None and start < point[0] <= end + 1e-9:
                elapsed = point[0] - previous[0]
                if elapsed > 0:
                    rate = (point[1] - previous[1]) / elapsed
                    best = rate if best is None else max(best, rate)
            previous = point
        return best

    def features(self):
        """Forecaster inputs at the latest point, or None until two points exist."""
        current = self.rate(1)
        if current is None:
            return None
        now = self.now
        five = self.rate(5)
        earlier = self.rate(5, now - 5)
        slope = 0.0 if earlier is None else (five - earlier) / 5
        starts, ends = self.sale_starts, self.sale_ends
        finished = [end - start for start, end in zip(starts, ends, strict=False)]
        gaps = [later - earlier for earlier, later in zip(starts, starts[1:], strict=False)]
        values = {
            "rate_1": current,
            "rate_5": five,
            "rate_15": self.rate(15),
            "rate_60": self.rate(60),
            "slope_5": slope,
            "lag": float(self.lag),
        }
        scaled = {key: value / self.capacity for key, value in values.items()}
        scaled.update(
            {
                "in_sale": 1.0 if self.in_sale else 0.0,
                "since_sale_start": now - starts[-1] if starts else NEVER,
                "since_sale_end": now - ends[-1] if ends else NEVER,
                "last_sale_seconds": finished[-1] if finished else NEVER,
                "last_gap_seconds": gaps[-1] if gaps else NEVER,
                "sales_seen": float(len(starts)),
            }
        )
        return [scaled[name] for name in FEATURES]

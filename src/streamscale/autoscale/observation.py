"""One controller input sample, built from exporter metrics and the Deployment status."""

from dataclasses import dataclass

from prometheus_client.parser import text_string_to_metric_families

PARTITIONS = 3


@dataclass(frozen=True)
class Observation:
    """Signals available to a policy at time ``at`` (seconds, any monotonic origin).

    ``highs`` are per-partition broker high watermarks; arrivals are their differences.
    ``fresh_at`` is when the exporter last sampled Kafka, on the same clock as ``at``.
    """

    at: float
    highs: tuple[int, ...]
    lags: tuple[int, ...]
    ready: int
    desired: int
    fresh_at: float | None = None
    # Live Kafka consumer-group members; None when the source cannot report it.
    members: int | None = None

    def __post_init__(self):
        if len(self.highs) != PARTITIONS or len(self.lags) != PARTITIONS:
            raise ValueError(f"Observations need exactly {PARTITIONS} partitions")
        if min(self.highs) < 0 or min(self.lags) < 0:
            raise ValueError("Offsets and lag must be nonnegative")
        if self.ready < 0 or self.desired < 0:
            raise ValueError("Replica counts must be nonnegative")

    @property
    def high(self) -> int:
        return sum(self.highs)

    @property
    def lag(self) -> int:
        return sum(self.lags)

    def to_dict(self):
        return {
            "at": self.at,
            "highs": list(self.highs),
            "lags": list(self.lags),
            "ready": self.ready,
            "desired": self.desired,
            "fresh_at": self.fresh_at,
            "members": self.members,
        }

    @classmethod
    def from_dict(cls, value):
        return cls(
            at=float(value["at"]),
            highs=tuple(int(item) for item in value["highs"]),
            lags=tuple(int(item) for item in value["lags"]),
            ready=int(value["ready"]),
            desired=int(value["desired"]),
            fresh_at=None if value.get("fresh_at") is None else float(value["fresh_at"]),
            members=None if value.get("members") is None else int(value["members"]),
        )


@dataclass(frozen=True)
class ExporterSample:
    run_id: str | None
    mode: str | None
    scenario: str | None
    highs: tuple[int, ...]
    lags: tuple[int, ...]
    success: float | None
    members: int | None = None


def exporter_sample(payload: str, run_id: str | None = None) -> ExporterSample:
    """Parse exporter /metrics text into one run's Kafka signals.

    Raises ValueError when any partition is missing, so a policy never acts on partial data.
    """
    highs, lags, success, members, identity = {}, {}, None, None, {}
    for family in text_string_to_metric_families(payload):
        for sample in family.samples:
            if run_id is not None and sample.labels.get("run_id") not in {None, run_id}:
                continue
            if sample.name == "streamscale_kafka_high_watermark":
                highs[int(sample.labels["partition"])] = int(sample.value)
            elif sample.name == "streamscale_kafka_lag":
                lags[int(sample.labels["partition"])] = int(sample.value)
            elif sample.name == "streamscale_exporter_last_success_unixtime":
                success = float(sample.value)
            elif sample.name == "streamscale_consumer_replicas":
                members = int(sample.value)
            else:
                continue
            for key in ("run_id", "mode", "scenario"):
                if key in sample.labels:
                    identity.setdefault(key, set()).add(sample.labels[key])
    expected = set(range(PARTITIONS))
    if set(highs) != expected or set(lags) != expected:
        raise ValueError("Exporter metrics do not cover all partitions yet")
    if any(len(values) > 1 for values in identity.values()):
        raise ValueError("Exporter metrics mix more than one run")
    return ExporterSample(
        run_id=next(iter(identity.get("run_id", {None}))),
        mode=next(iter(identity.get("mode", {None}))),
        scenario=next(iter(identity.get("scenario", {None}))),
        highs=tuple(highs[part] for part in range(PARTITIONS)),
        lags=tuple(lags[part] for part in range(PARTITIONS)),
        success=success,
        members=members,
    )


def parse_exporter(payload: str, run_id: str | None = None):
    """Return (highs, lags, last_success_unixtime) from exporter /metrics text."""
    sample = exporter_sample(payload, run_id)
    return sample.highs, sample.lags, sample.success

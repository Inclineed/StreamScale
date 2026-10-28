"""One run per process; durable workload totals belong to the singleton exporter."""

import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    GCCollector,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)
from prometheus_client.core import CounterMetricFamily

from streamscale.contract import timestamp

LABELS = ("run_id", "mode", "scenario")
BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120)


def registry():
    result = CollectorRegistry()
    GCCollector(registry=result)
    PlatformCollector(registry=result)
    ProcessCollector(registry=result)
    return result


def labels(run):
    return tuple(str(run[field]) for field in LABELS)


class ConsumerMetrics:
    def __init__(self, run: dict, collection: CollectorRegistry):
        values = labels(run)
        for outcome in ("accepted", "duplicate", "invalid", "dlq", "late"):
            counter = Counter(
                f"streamscale_{outcome}_total",
                f"Durable/attempt {outcome} outcomes",
                LABELS,
                registry=collection,
            ).labels(*values)
            setattr(self, outcome, counter)
        self.failed = Counter(
            "streamscale_failed_total",
            "Handling failures by stage",
            (*LABELS, "stage"),
            registry=collection,
        )
        self.values = values
        for stage in ("db", "dlq", "offset", "ownership"):
            self.failed.labels(*values, stage)
        self.db = Histogram(
            "streamscale_db_seconds",
            "Complete DB handling including retries",
            (*LABELS, "result"),
            buckets=BUCKETS,
            registry=collection,
        )
        for outcome in ("accepted", "duplicate", "failed"):
            self.db.labels(*values, outcome)
        self.e2e = Histogram(
            "streamscale_end_to_end_seconds",
            "Actual produced_at to successful unique DB commit",
            LABELS,
            buckets=BUCKETS,
            registry=collection,
        ).labels(*values)
        self.assignments = Counter(
            "streamscale_assignments_total", "Assignment callbacks", LABELS, registry=collection
        ).labels(*values)
        self.owned = Gauge(
            "streamscale_assigned_partitions",
            "Partitions owned by this process",
            LABELS,
            registry=collection,
        ).labels(*values)

    def database(self, elapsed, result, event=None):
        self.db.labels(*self.values, result).observe(elapsed)
        if result in {"accepted", "duplicate"}:
            getattr(self, result).inc()
        if result == "accepted":
            self.e2e.observe(
                max(0, (datetime.now(UTC) - timestamp(event["produced_at"])).total_seconds())
            )

    def failure(self, stage):
        self.failed.labels(*self.values, stage).inc()


class WorkloadTotals:
    """Artifact-backed counters: constant across exporter restarts and consumer replicas."""

    def __init__(self, run):
        self.run = run

    def collect(self):
        for name, field in (("generated", "attempts"), ("published", "acknowledged")):
            metric = CounterMetricFamily(
                f"streamscale_{name}", "Durable published execution totals", labels=LABELS
            )
            metric.add_metric(labels(self.run), self.run.get(field, 0))
            yield metric


class MetricsServer:
    def __init__(self, collection, port, host="0.0.0.0", routes=None):
        """``routes`` maps extra GET paths to callables returning (bytes, content type)."""
        self.ready = Event()
        ready = self.ready
        extra = dict(routes or {})

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path in extra:
                    payload, content = extra[self.path]()
                    status = 200
                elif self.path == "/metrics":
                    payload, status, content = generate_latest(collection), 200, CONTENT_TYPE_LATEST
                elif self.path == "/health/live":
                    payload, status, content = b"live\n", 200, "text/plain"
                elif self.path == "/health/ready":
                    payload, status, content = (
                        b"ready\n",
                        200 if ready.is_set() else 503,
                        "text/plain",
                    )
                else:
                    payload, status, content = b"not found\n", 404, "text/plain"
                self.send_response(status)
                self.send_header("Content-Type", content)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer((host, port), Handler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.ready.clear()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


def measured_database(metrics, store, event, partition, offset):
    started = time.monotonic()
    try:
        result = store.persist(event, partition, offset)
    except Exception:
        if metrics:
            metrics.database(time.monotonic() - started, "failed")
            metrics.failure("db")
        raise
    if metrics:
        metrics.database(time.monotonic() - started, result.outcome, event)
        if result.outcome == "accepted" and not result.state_applied:
            metrics.late.inc()
    return result

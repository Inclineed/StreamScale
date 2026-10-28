"""Long-running consumer and singleton Kafka/run exporter, sharing the same image."""

import time
from pathlib import Path
from threading import Event

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from prometheus_client import Counter, Gauge

from streamscale.config import Settings
from streamscale.consumer import consume
from streamscale.metrics import (
    LABELS,
    ConsumerMetrics,
    MetricsServer,
    WorkloadTotals,
    labels,
    registry,
)
from streamscale.migrations import migrate
from streamscale.run_artifacts import read_run
from streamscale.topics import INPUT_TOPIC, ensure_topics


def bootstrap(settings: Settings, timeout=180):
    deadline = time.monotonic() + timeout
    while True:
        try:
            topics = ensure_topics(settings.kafka_bootstrap_servers)
            applied = migrate(settings.database_url)
            return {"topics": topics, "applied": applied}
        except Exception:
            if time.monotonic() >= deadline:
                raise RuntimeError("Kafka/database bootstrap did not become ready") from None
            time.sleep(2)


def wait_run(directory: Path, stop: Event, timeout=300, allow_publishing=False):
    deadline = time.monotonic() + timeout
    while not stop.is_set():
        try:
            run = read_run(directory)
        except FileNotFoundError:
            run = None
        accepted = {"publishing", "published"} if allow_publishing else {"published"}
        if run is not None and run["status"] in accepted:
            return run
        if run is not None and run["status"] == "failed":
            raise RuntimeError("The cluster workload failed; inspect the workload Job logs")
        if time.monotonic() >= deadline:
            raise RuntimeError("Published cluster run was not available before the deadline")
        stop.wait(1)
    return None


def serve_consumer(settings, directory, port, stop):
    collection = registry()
    server = MetricsServer(collection, port)
    try:
        run = wait_run(directory, stop, allow_publishing=True)
        if run is None:
            return {"status": "stopped"}
        return consume(
            settings,
            directory=directory,
            stay_running=True,
            stop=stop,
            metrics=ConsumerMetrics(run, collection),
            readiness=server.ready,
            follow_publish=True,
        )
    finally:
        server.close()


def kafka_snapshot(consumer, admin, run):
    lag, _highs, replicas = kafka_sample(consumer, admin, run)
    return lag, replicas


def kafka_sample(consumer, admin, run):
    """Per-partition lag, broker high watermarks and live group members.

    High watermarks let a scaling policy measure arrivals from Kafka itself instead of the
    run's planned schedule.
    """
    parts = [TopicPartition(INPUT_TOPIC, part) for part in range(3)]
    committed = consumer.committed(parts, timeout=10)
    lag = {}
    highs = {}
    for part in committed:
        low, high = consumer.get_watermark_offsets(part, timeout=10, cached=False)
        position = (
            part.offset if part.offset >= 0 else max(low, run["start_offsets"][str(part.partition)])
        )
        lag[part.partition] = max(0, high - position)
        highs[part.partition] = high
    group = admin.describe_consumer_groups([run["group_id"]], request_timeout=10)
    try:
        replicas = len(group[run["group_id"]].result(timeout=15).members)
    except KafkaException as error:
        # Treat a not-yet-created group as zero; propagate all other failures.
        if error.args[0].code() != KafkaError.GROUP_ID_NOT_FOUND:
            raise
        replicas = 0
    return lag, highs, replicas


def serve_exporter(settings, directory, port, stop):
    collection = registry()
    server = MetricsServer(collection, port)
    probe = None
    try:
        run = wait_run(directory, stop, allow_publishing=True)
        if run is None:
            return {"status": "stopped"}
        values = labels(run)
        collection.register(WorkloadTotals(run))
        lag = Gauge(
            "streamscale_kafka_lag",
            "Broker high offset minus group committed offset",
            (*LABELS, "partition"),
            registry=collection,
        )
        high_watermark = Gauge(
            "streamscale_kafka_high_watermark",
            "Broker high watermark (next offset to be written) of the input topic",
            (*LABELS, "partition"),
            registry=collection,
        )
        replicas = Gauge(
            "streamscale_consumer_replicas",
            "Live members in this run's Kafka group",
            LABELS,
            registry=collection,
        ).labels(*values)
        success = Gauge(
            "streamscale_exporter_last_success_unixtime",
            "Last fresh Kafka sample",
            LABELS,
            registry=collection,
        ).labels(*values)
        errors = Counter(
            "streamscale_exporter_errors_total", "Failed Kafka samples", LABELS, registry=collection
        ).labels(*values)
        probe = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": run["group_id"],
                "enable.auto.commit": False,
            }
        )
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        while not stop.is_set():
            try:
                if run["status"] == "publishing":
                    current = read_run(directory)
                    if current["run_id"] != run["run_id"]:
                        raise RuntimeError("Active metrics run identity changed")
                    run.update(current)
                positions, highs, members = kafka_sample(probe, admin, run)
                for part, pending in positions.items():
                    lag.labels(*values, str(part)).set(pending)
                for part, high in highs.items():
                    high_watermark.labels(*values, str(part)).set(high)
                replicas.set(members)
                success.set(time.time())
                server.ready.set()
            except Exception as error:
                # No DSNs/payloads in logs. Retained samples have an explicit freshness gauge.
                print(f"Kafka metrics sample failed: {type(error).__name__}", flush=True)
                errors.inc()
                server.ready.clear()
            stop.wait(1)
        return {"status": "stopped", "run_id": run["run_id"]}
    finally:
        if probe is not None:
            probe.close()
        server.close()

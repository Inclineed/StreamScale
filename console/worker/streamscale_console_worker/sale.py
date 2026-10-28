"""Interactive sale: seeded manifest generation and a controllable, acknowledged publisher.

The manifest comes from the unchanged generator's logical workload, with the
console's chosen malformed/duplicate/late percentages applied by the same seeded
method as the correctness profile. Records use the unchanged wire format and
validation, and only broker acknowledgements are logged, exactly as the measured
publisher does. Pacing differs: the virtual schedule clock can pause, and a live
rate can replace it.

Every finished sale, including a stopped, aborted or interrupted one, is finalized
as ``published`` with its real end offsets, so the unchanged consumer always drains
it. The console-specific outcome lives under ``run["console"]``.
"""

import hashlib
import json
import os
import random
import threading
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import psycopg
from confluent_kafka import KafkaError, Producer
from psycopg.types.json import Jsonb
from streamscale_workload.generator import INVALID_FAULTS, logical_workload, wire_record

from streamscale.contract import ContractError, timestamp, validate_event
from streamscale.ids import MAX_SEED, new_run_id
from streamscale.run_artifacts import (
    manifest_hash,
    offsets,
    read_jsonl,
    read_run,
    revision_snapshot,
    utc_now,
    write_json,
)
from streamscale.topics import DLQ_TOPIC, INPUT_TOPIC, PARTITIONS, ensure_topics, producer_config
from streamscale_console_worker import control

MAX_RATE = 2000
MAX_EVENTS = 400_000
MAX_SECONDS = 3600
LATE_DELAY_US = 10_000_000
SCENARIOS = ("steady", "flash-sale", "overload-recovery", "ramp", "repeated-sales")
MODES = ("fixed", "manual", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive")
FAULT_KEYS = ("malformed_pct", "duplicate_pct", "late_pct")
ACTIVE = {"preparing", "publishing", "paused", "stopping"}


class SaleError(ValueError):
    """A sale request that cannot be accepted; safe to show."""


def validate_config(config):
    if not isinstance(config, dict):
        raise SaleError("Sale configuration must be an object")
    scenario = config.get("scenario")
    if scenario not in SCENARIOS:
        raise SaleError("Scenario must be one of " + ", ".join(SCENARIOS))
    seed = config.get("seed")
    if type(seed) is not int or not 0 <= seed <= MAX_SEED:
        raise SaleError("Seed must be an integer from 0 to 2^63-1")
    schedule = config.get("schedule")
    if (
        not isinstance(schedule, list)
        or not 1 <= len(schedule) <= 12
        or any(
            not isinstance(phase, list)
            or len(phase) != 2
            or any(type(value) is not int for value in phase)
            or not 1 <= phase[0] <= MAX_SECONDS
            or not 1 <= phase[1] <= MAX_RATE
            for phase in schedule
        )
    ):
        raise SaleError(
            f"Schedule needs 1–12 phases of [seconds 1–{MAX_SECONDS}, events/s 1–{MAX_RATE}]"
        )
    if sum(seconds for seconds, _ in schedule) > MAX_SECONDS:
        raise SaleError("A sale may last at most one hour")
    events = sum(seconds * rate for seconds, rate in schedule)
    if not 2 <= events <= MAX_EVENTS:
        raise SaleError(f"The schedule must produce 2–{MAX_EVENTS:,} events")
    faults = config.get("faults") or {}
    if not isinstance(faults, dict) or set(faults) - set(FAULT_KEYS):
        raise SaleError("Fault mix accepts malformed_pct, duplicate_pct and late_pct")
    mix = {}
    for key in FAULT_KEYS:
        value = faults.get(key, 0)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= value <= 20:
            raise SaleError("Each fault percentage must be between 0 and 20")
        mix[key] = round(float(value), 2)
    mode = config.get("mode", "fixed")
    if mode not in MODES:
        raise SaleError("Mode must be one of " + ", ".join(MODES))
    preset = str(config.get("preset", "custom"))[:40]
    session = str(config.get("session", ""))[:80]
    return {
        "scenario": scenario,
        "seed": seed,
        "schedule": schedule,
        "orders": events // 2,
        "faults": mix,
        "mode": mode,
        "preset": preset,
        "session": session,
    }


def console_workload(config, run_id):
    """Logical workload with the console's seeded fault mix (correctness-profile method)."""
    attempts = logical_workload(
        config["seed"], config["orders"], config["scenario"], "normal", run_id, config["schedule"]
    )
    # A normal-profile manifest is in logical order, so list positions are logical indexes.
    count = len(attempts)
    seed = config["seed"]
    mix = config["faults"]
    invalid_count = round(count * mix["malformed_pct"] / 100)
    invalid = set(random.Random(f"console-v1:{seed}:invalid").sample(range(count), invalid_count))
    valid = [index for index in range(count) if index not in invalid]
    late = random.Random(f"console-v1:{seed}:late").sample(
        valid, min(len(valid), round(count * mix["late_pct"] / 100))
    )
    duplicate = random.Random(f"console-v1:{seed}:duplicate").sample(
        valid, min(len(valid), round(count * mix["duplicate_pct"] / 100))
    )
    for position, index in enumerate(sorted(invalid)):
        attempts[index]["fault"] = INVALID_FAULTS[position % len(INVALID_FAULTS)]
    for index in late:
        attempts[index]["fault"] = "late"
        attempts[index]["scheduled_at_us"] += LATE_DELAY_US
    for index in duplicate:
        attempts.append(
            attempts[index]
            | {"duplicate": True, "scheduled_at_us": attempts[index]["scheduled_at_us"] + 500}
        )
    attempts.sort(
        key=lambda attempt: (
            attempt["scheduled_at_us"],
            attempt["logical_index"],
            attempt["duplicate"],
        )
    )
    return [attempt | {"attempt_index": index} for index, attempt in enumerate(attempts)]


def run_name_for(now=None):
    stamp = (now or datetime.now(UTC)).strftime("%Y%m%d%H%M%S")
    return f"sale-{stamp}-{os.urandom(4).hex()}"


def mode_label(mode):
    """experiment_run.mode only admits measured modes; manual sales are recorded as fixed.

    Migration 0002 added ml-reactive and ml-predictive, so they keep their own label.
    """
    return "fixed" if mode == "manual" else mode


class Pacer:
    """Schedule clock that can pause, or a live rate that replaces it."""

    def __init__(self, attempts):
        self.attempts = attempts
        self.virtual_us = 0.0
        self.rate = None
        self.tokens = 0.0
        self.last = None

    def set_rate(self, rate, cursor):
        if rate is None:
            # Back to the schedule from wherever the cursor is.
            if cursor < len(self.attempts):
                self.virtual_us = max(self.virtual_us, self.attempts[cursor]["scheduled_at_us"])
            self.rate = None
        else:
            self.rate = float(rate)
        self.tokens = 0.0

    def due(self, cursor, now, paused):
        if self.last is None:
            self.last = now
        elapsed = max(0.0, now - self.last)
        self.last = now
        if paused or cursor >= len(self.attempts):
            return cursor
        if self.rate is None:
            self.virtual_us += elapsed * 1_000_000
            limit = cursor
            while (
                limit < len(self.attempts)
                and self.attempts[limit]["scheduled_at_us"] <= self.virtual_us
            ):
                limit += 1
            return limit
        self.tokens = min(self.tokens + elapsed * self.rate, max(self.rate, 1.0))
        whole = int(self.tokens)
        self.tokens -= whole
        return min(len(self.attempts), cursor + whole)

    def current_rate(self, cursor):
        if self.rate is not None:
            return self.rate
        if cursor >= len(self.attempts):
            return 0.0
        horizon = self.virtual_us + 1_000_000
        ahead = sum(
            1
            for attempt in self.attempts[cursor : cursor + MAX_RATE * 2]
            if attempt["scheduled_at_us"] <= horizon
        )
        return float(ahead)


class Sale:
    """One sale's publisher thread and its control flags."""

    def __init__(self, settings, config, root=control.ROOT, on_finish=None):
        self.settings = settings
        self.config = config
        self.root = root
        self.on_finish = on_finish
        self.run_id = new_run_id()
        self.run_name = run_name_for()
        self.directory = root / "runs" / self.run_name
        self.state = "preparing"
        self.error = None
        self.lock = threading.Lock()
        self.pause_requested = False
        self.stop_requested = None  # None, "stop" or "abort"
        self.rate_request = ("schedule", None)
        self.attempts = []
        self.cursor = 0
        self.acknowledged = 0
        self.failures = []
        self.purged = 0
        self.buffer_full = False
        self.started_at = None
        self.ended_at = None
        self.history = []
        self.recent = deque(maxlen=12)
        self.partition_acks = [0] * PARTITIONS
        self.pacer = None
        self.thread = threading.Thread(target=self._run, name="sale", daemon=True)

    # Commands -----------------------------------------------------------------

    def start(self):
        self.prepare()
        self.thread.start()

    def pause(self):
        with self.lock:
            if self.state not in {"publishing", "paused"}:
                raise SaleError("Only a publishing sale can pause")
            self.pause_requested = True
            self.history.append({"at": time.time(), "action": "pause"})

    def resume(self):
        with self.lock:
            if self.state not in {"publishing", "paused"}:
                raise SaleError("Only a paused sale can resume")
            self.pause_requested = False
            self.history.append({"at": time.time(), "action": "resume"})

    def set_rate(self, rate):
        if rate is not None and (type(rate) is not int or not 1 <= rate <= MAX_RATE):
            raise SaleError(f"Rate must be an integer from 1 to {MAX_RATE}, or the schedule")
        with self.lock:
            if self.state not in {"publishing", "paused"}:
                raise SaleError("The rate can change only while the sale is publishing or paused")
            self.rate_request = ("rate" if rate else "schedule", rate)
            self.history.append({"at": time.time(), "action": "rate", "rate": rate})

    def stop(self, abort=False):
        with self.lock:
            if self.state not in {"preparing", "publishing", "paused"}:
                raise SaleError("This sale is no longer publishing")
            self.stop_requested = "abort" if abort else "stop"
            self.history.append({"at": time.time(), "action": self.stop_requested})

    # Work ---------------------------------------------------------------------

    def prepare(self):
        self.attempts = console_workload(self.config, self.run_id)
        self.directory.mkdir(parents=True, exist_ok=False)
        with (self.directory / "manifest.jsonl").open("w", encoding="utf-8") as stream:
            for attempt in self.attempts:
                stream.write(json.dumps(attempt, sort_keys=True) + "\n")
        late = sum(attempt["fault"] == "late" for attempt in self.attempts)
        self.run = {
            "artifact_version": 1,
            "generator_version": 2,
            "console_generator_version": 1,
            "run_id": str(self.run_id),
            "group_id": f"streamscale.console.{self.run_name}",
            "seed": self.config["seed"],
            "orders": self.config["orders"],
            "scenario": self.config["scenario"],
            "profile": "console",
            "mode": mode_label(self.config["mode"]),
            "publishing_schedule": self.config["schedule"],
            "experiment_configuration": {"interactive": True} | self.config,
            "created_at": utc_now(),
            "status": "generated",
            "attempts": len(self.attempts),
            "valid_unique": len(
                {a["event_id"] for a in self.attempts if not a["fault"].startswith("invalid_")}
            ),
            "invalid_attempts": sum(a["fault"].startswith("invalid_") for a in self.attempts),
            "duplicate_attempts": sum(a["duplicate"] for a in self.attempts),
            "manifest_sha256": manifest_hash(self.attempts),
            "logical_sha256": manifest_hash(self.attempts, logical=True),
            "console": {
                "interactive": True,
                "session": self.config["session"],
                "preset": self.config["preset"],
                "start_mode": self.config["mode"],
                "fault_mix": self.config["faults"],
                "late_attempts": late,
                "outcome": None,
            },
        }
        write_json(self.directory / "run.json", self.run)

    def _run(self):
        try:
            self._publish()
        except Exception as error:  # noqa: BLE001 - recorded on the run and reported
            self.error = f"{type(error).__name__}: {str(error)[:200]}"
            self.state = "failed"
            self.ended_at = time.time()
            try:
                self.finalize(outcome="failed")
            except Exception:  # noqa: BLE001
                pass
        finally:
            if self.on_finish:
                self.on_finish(self)

    def _publish(self):
        settings = self.settings
        ensure_topics(settings.kafka_bootstrap_servers)
        run = self.run
        run.update(revision_snapshot())
        run["start_offsets"] = offsets(settings.kafka_bootstrap_servers, INPUT_TOPIC)
        run["dlq_start_offsets"] = offsets(settings.kafka_bootstrap_servers, DLQ_TOPIC)
        run["publish_started_at"] = utc_now()
        run["paced"] = True
        run["status"] = "publishing"
        run["acknowledged"] = 0
        write_json(self.directory / "run.json", run)
        with psycopg.connect(settings.database_url, connect_timeout=10) as connection:
            connection.execute(
                """INSERT INTO experiment_run
                   (run_id, mode, scenario, seed, started_at, git_commit, configuration)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    UUID(run["run_id"]),
                    run["mode"],
                    run["scenario"],
                    run["seed"],
                    timestamp(run["publish_started_at"]),
                    run["git_commit"],
                    Jsonb(run),
                ),
            )
        # Consumers follow this run only once it is registered and its range starts.
        control.set_current_run(self.run_name, self.root)
        # Buffer through short broker outages instead of failing deliveries.
        producer = Producer(
            producer_config(settings.kafka_bootstrap_servers)
            | {"message.timeout.ms": 180000, "linger.ms": 5}
        )
        anchor = timestamp(run["publish_started_at"])
        self.started_at = time.time()
        self.pacer = Pacer(self.attempts)
        with self.lock:
            self.state = "publishing"
        last_checkpoint = time.monotonic()
        with (self.directory / "acknowledgements.jsonl").open("x", encoding="utf-8") as log:

            def delivery(attempt, key, payload, produced_at):
                def acknowledged(error, message):
                    if error is not None:
                        if error.code() == KafkaError._PURGE_QUEUE:
                            self.purged += 1
                        else:
                            self.failures.append(
                                {"attempt_index": attempt["attempt_index"], "error": error.name()}
                            )
                        return
                    log.write(
                        json.dumps(
                            {
                                "attempt_index": attempt["attempt_index"],
                                "event_id": attempt["event_id"],
                                "valid": not attempt["fault"].startswith("invalid_"),
                                "duplicate": attempt["duplicate"],
                                "fault": attempt["fault"],
                                "produced_at": produced_at,
                                "partition": message.partition(),
                                "offset": message.offset(),
                                "key_base64": _encoded(key),
                                "payload_base64": _encoded(payload),
                            },
                            sort_keys=True,
                        )
                        + "\n"
                    )
                    self.acknowledged += 1
                    self.partition_acks[message.partition()] += 1
                    self.recent.appendleft(
                        {
                            "event_id": attempt["event_id"],
                            "order_id": attempt["order_id"],
                            "event_type": attempt["event_type"],
                            "fault": attempt["fault"],
                            "duplicate": attempt["duplicate"],
                            "partition": message.partition(),
                            "offset": message.offset(),
                        }
                    )

                return acknowledged

            while True:
                with self.lock:
                    paused = self.pause_requested
                    stop = self.stop_requested
                    request, self.rate_request = self.rate_request, None
                    if not stop:
                        self.state = "paused" if paused else "publishing"
                if request is not None:
                    self.pacer.set_rate(request[1], self.cursor)
                if stop or self.cursor >= len(self.attempts):
                    break
                limit = self.pacer.due(self.cursor, time.monotonic(), paused)
                while self.cursor < limit:
                    attempt = self.attempts[self.cursor]
                    if not self._produce(producer, attempt, anchor, delivery):
                        break
                    self.cursor += 1
                producer.poll(0)
                if time.monotonic() - last_checkpoint >= 1:
                    log.flush()
                    run["acknowledged"] = self.acknowledged
                    write_json(self.directory / "run.json", run)
                    last_checkpoint = time.monotonic()
                producer.poll(0.005 if not paused else 0.1)
            with self.lock:
                self.state = "stopping"
            if self.stop_requested == "abort":
                # Unsent records never reach the broker; in-flight ones still finish.
                producer.purge(in_queue=True, in_flight=False, blocking=True)
            remaining = producer.flush(180)
            log.flush()
            os.fsync(log.fileno())
        if remaining:
            self.failures.append({"attempt_index": None, "error": f"{remaining} unflushed"})
        outcome = {None: "completed", "stop": "stopped", "abort": "aborted"}[self.stop_requested]
        if outcome == "completed" and self.cursor < len(self.attempts):
            outcome = "stopped"
        self.finalize(outcome)

    def _produce(self, producer, attempt, anchor, delivery):
        deadline = time.monotonic() + 30
        while True:
            produced_at = utc_now()
            key, payload = wire_record(attempt, self.run["run_id"], anchor, produced_at)
            try:
                validate_event(payload, key)
            except ContractError:
                if not attempt["fault"].startswith("invalid_"):
                    raise
            else:
                if attempt["fault"].startswith("invalid_"):
                    raise RuntimeError("Requested fault unexpectedly produced a valid event")
            try:
                producer.produce(
                    INPUT_TOPIC,
                    key=key,
                    value=payload,
                    on_delivery=delivery(attempt, key, payload, produced_at),
                )
                self.buffer_full = False
                return True
            except BufferError:
                # Broker unavailable or slow: wait, keep serving callbacks, honour stop.
                self.buffer_full = True
                producer.poll(0.1)
                if self.stop_requested or time.monotonic() >= deadline:
                    return False

    def finalize(self, outcome):
        run = self.run
        settings = self.settings
        path = self.directory / "acknowledgements.jsonl"
        if not path.exists():
            path.touch()
        _drop_partial_line(path)
        acknowledged = len(read_jsonl(path))
        try:
            end = offsets(settings.kafka_bootstrap_servers, INPUT_TOPIC)
        except Exception:  # noqa: BLE001
            end = None
        if "start_offsets" not in run:
            run["status"] = "failed"
        else:
            run["end_offsets"] = end or {
                part: run["start_offsets"][part] for part in run["start_offsets"]
            }
            run["status"] = "published"
        in_log = (
            sum(run["end_offsets"][part] - start for part, start in run["start_offsets"].items())
            if "start_offsets" in run
            else 0
        )
        run["publish_ended_at"] = utc_now()
        run["acknowledged"] = acknowledged
        run["acknowledgements_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        run["console"].update(
            {
                "outcome": outcome,
                "attempted": self.cursor,
                "unsent_attempts": len(self.attempts) - self.cursor,
                "failed_deliveries": len(self.failures),
                "delivery_failures": self.failures[:200],
                "purged": self.purged,
                # Records in the captured range without a logged acknowledgement.
                "unacknowledged_in_log": in_log - acknowledged,
                "control_history": self.history,
                "error": self.error,
            }
        )
        if run["status"] == "published":
            try:
                with psycopg.connect(settings.database_url, connect_timeout=10) as connection:
                    connection.execute(
                        "UPDATE experiment_run SET configuration = %s WHERE run_id = %s",
                        (Jsonb(run), UUID(run["run_id"])),
                    )
            except psycopg.Error:
                run["console"]["sql_update_failed"] = True
        write_json(self.directory / "run.json", run)
        with self.lock:
            self.state = "finished" if run["status"] == "published" else "failed"
        self.ended_at = time.time()

    def status(self):
        with self.lock:
            state = self.state
        pacer = self.pacer
        return {
            "state": state,
            "run_name": self.run_name,
            "run_id": str(self.run_id),
            "group_id": self.run.get("group_id") if hasattr(self, "run") else None,
            "config": self.config,
            "budget": len(self.attempts),
            "attempted": self.cursor,
            "acknowledged": self.acknowledged,
            "failed": len(self.failures),
            "purged": self.purged,
            "buffer_full": self.buffer_full,
            "rate_mode": "schedule" if pacer is None or pacer.rate is None else "manual",
            "target_rate": None if pacer is None else pacer.current_rate(self.cursor),
            "schedule_seconds": None if pacer is None else pacer.virtual_us / 1_000_000,
            "paused": self.pause_requested,
            "stop_requested": self.stop_requested,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "outcome": self.run.get("console", {}).get("outcome") if hasattr(self, "run") else None,
            "error": self.error,
            "history": self.history[-50:],
            "recent": list(self.recent),
            "partition_acks": list(self.partition_acks),
        }


def recover_interrupted(settings, root=control.ROOT):
    """Finalize a sale whose agent died mid-publication; never republish it."""
    pointer = control.current_run(root)
    if pointer is None:
        return None
    directory = Path(pointer["run_dir"])
    run = read_run(directory)
    if run.get("profile") != "console" or run["status"] not in {"generated", "publishing"}:
        return None
    sale = Sale.__new__(Sale)
    sale.settings = settings
    sale.root = root
    sale.directory = directory
    sale.run = run
    sale.run_id = UUID(run["run_id"])
    sale.run_name = pointer["run_name"]
    sale.cursor = (
        len(read_jsonl(directory / "acknowledgements.jsonl"))
        if (directory / "acknowledgements.jsonl").exists()
        else 0
    )
    sale.attempts = read_jsonl(directory / "manifest.jsonl")
    sale.failures = []
    sale.purged = 0
    sale.recent = deque(maxlen=12)
    sale.partition_acks = [0] * PARTITIONS
    sale.history = run.get("console", {}).get("control_history", [])
    sale.error = "agent restarted during publication"
    sale.lock = threading.Lock()
    sale.state = "stopping"
    sale.finalize("interrupted")
    run = read_run(directory)
    run["console"]["recovered_at"] = utc_now()
    write_json(directory / "run.json", run)
    return run


def _drop_partial_line(path):
    data = path.read_bytes()
    if data and not data.endswith(b"\n"):
        path.write_bytes(data[: data.rfind(b"\n") + 1])


def _encoded(value):
    import base64

    return None if value is None else base64.b64encode(value).decode("ascii")


class SaleComponent:
    """Agent commands for the current sale; one sale at a time."""

    def __init__(self, agent):
        self.agent = agent
        self.sale = None
        self.recovered = None
        self.listeners = []
        self.commands = {
            "sale.start": self.start,
            "sale.pause": lambda body: self._current().pause(),
            "sale.resume": lambda body: self._current().resume(),
            "sale.rate": lambda body: self._current().set_rate(body.get("rate")),
            "sale.stop": lambda body: self._current().stop(abort=False),
            "sale.abort": lambda body: self._current().stop(abort=True),
        }
        self.busy = [self.active]
        self.sections = {"sale": self.section}
        self.runners = [self.recover]

    def recover(self, stop):
        try:
            self.recovered = recover_interrupted(self.agent.settings, self.agent.root)
        except Exception as error:  # noqa: BLE001
            self.recovered = {"error": type(error).__name__}

    def active(self):
        return self.sale is not None and self.sale.state in ACTIVE

    def _current(self):
        if self.sale is None:
            raise SaleError("No sale has been started")
        return self.sale

    def start(self, body):
        if self.active():
            raise SaleError("A sale is already publishing; stop it first")
        config = validate_config(body.get("config"))
        sale = Sale(self.agent.settings, config, self.agent.root, on_finish=self.finished)
        sale.start()
        self.sale = sale
        for listener in self.listeners:
            listener(sale)
        return {
            "run_name": sale.run_name,
            "run_id": str(sale.run_id),
            "group_id": sale.run["group_id"],
            "attempts": len(sale.attempts),
        }

    def finished(self, sale):
        pass

    def section(self):
        if self.sale is None:
            return {"state": "idle", "recovered": self.recovered}
        return self.sale.status() | {"recovered": self.recovered}


def component(agent):
    return SaleComponent(agent)


__all__ = ["PARTITIONS", "SaleComponent", "validate_config", "console_workload", "Pacer"]

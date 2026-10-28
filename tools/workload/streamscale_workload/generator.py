"""Seeded logical workload, with actual publish timestamps deferred to the publisher."""

import base64
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

from streamscale.contract import timestamp
from streamscale.ids import MAX_SEED, event_id, new_run_id, order_id
from streamscale.run_artifacts import manifest_hash, utc_now, write_json
from streamscale_workload.shapes import schedule as _shape

SCENARIOS = {
    "steady": [(120, 100)],
    "flash-sale": [(30, 100), (15, 1500), (75, 100)],
    "overload-recovery": [(60, 1500), (60, 100)],
    # Protocol v3 shapes (see shapes.py); measured runs always pass an explicit schedule.
    "ramp": [tuple(phase) for phase in _shape("ramp", "eval:0", 52, 390)],
    "repeated-sales": [tuple(phase) for phase in _shape("repeated-sales", "eval:0", 52, 390)],
}
INVALID_FAULTS = ("invalid_json", "invalid_encoding", "invalid_amount", "invalid_key")


def validate_schedule(schedule):
    if not isinstance(schedule, list) or not schedule:
        raise ValueError("Publishing schedule must be a nonempty list of [seconds, events/second]")
    if any(
        not isinstance(phase, (list, tuple))
        or len(phase) != 2
        or any(type(value) is not int or value <= 0 for value in phase)
        for phase in schedule
    ):
        raise ValueError("Schedule durations and rates must be positive integers")
    if sum(seconds * rate for seconds, rate in schedule) > 2_000_000:
        raise ValueError("Publishing schedule exceeds two million logical events")
    return schedule


def schedule_us(index: int, scenario: str, schedule=None) -> int:
    elapsed = 0
    for seconds, rate in SCENARIOS[scenario] if schedule is None else schedule:
        count = seconds * rate
        if index < count:
            return elapsed + index * 1_000_000 // rate
        index -= count
        elapsed += seconds * 1_000_000
    raise ValueError("Order count exceeds this scenario's publishing window")


def logical_workload(
    seed: int, orders: int, scenario: str, profile: str, run_id: UUID, schedule=None
) -> list[dict]:
    if type(seed) is not int or not 0 <= seed <= MAX_SEED:
        raise ValueError("Seed must be in [0, 2**63 - 1]")
    if type(orders) is not int or orders <= 0:
        raise ValueError("Order count must be a positive integer")
    if scenario not in SCENARIOS or profile not in {"normal", "correctness", "fixtures"}:
        raise ValueError("Unsupported scenario or fault profile")
    if profile == "fixtures" and orders != 2:
        raise ValueError("The fixtures profile requires --orders 2")
    if schedule is not None:
        validate_schedule(schedule)
    schedule_us(orders * 2 - 1, scenario, schedule)
    rng = random.Random(seed)
    attempts = []
    for order in range(orders):
        cents = rng.randrange(100_000_001)
        terminal = "PAYMENT_CONFIRMED" if rng.randrange(5) < 4 else "ORDER_CANCELLED"
        for kind in ("ORDER_CREATED", terminal):
            index = len(attempts)
            attempts.append(
                {
                    "logical_index": index,
                    "event_id": str(event_id(run_id, index)),
                    "order_id": str(order_id(seed, order)),
                    "event_type": kind,
                    "event_time_us": schedule_us(index, scenario, schedule),
                    "scheduled_at_us": schedule_us(index, scenario, schedule),
                    "order_amount": f"{cents // 100}.{cents % 100:02d}",
                    "fault": "none",
                    "duplicate": False,
                }
            )
    if profile == "correctness":
        count = len(attempts)
        invalid = set(random.Random(f"v1:{seed}:invalid").sample(range(count), count // 100))
        valid = [i for i in range(count) if i not in invalid]
        delayed = random.Random(f"v1:{seed}:late").sample(valid, count * 5 // 100)
        duplicate = random.Random(f"v1:{seed}:duplicate").sample(valid, count * 2 // 100)
        for position, index in enumerate(sorted(invalid)):
            attempts[index]["fault"] = INVALID_FAULTS[position % len(INVALID_FAULTS)]
        for index in delayed:
            attempts[index]["fault"] = "late"
            attempts[index]["scheduled_at_us"] += 10_000_000
        for index in duplicate:
            attempts.append(
                attempts[index]
                | {"duplicate": True, "scheduled_at_us": attempts[index]["scheduled_at_us"] + 500}
            )
    elif profile == "fixtures":
        # Order 0's created event arrives after its terminal event; order 1 ties business time.
        attempts[0]["fault"] = "late"
        attempts[0]["scheduled_at_us"] += 10_000_000
        attempts[3]["event_time_us"] = attempts[2]["event_time_us"]
        attempts.append(attempts[1] | {"duplicate": True, "scheduled_at_us": 40_000})
        for position, fault in enumerate(("invalid_json", "invalid_encoding")):
            attempts.append(
                attempts[1]
                | {
                    "logical_index": 4 + position,
                    "event_id": str(event_id(run_id, 4 + position)),
                    "fault": fault,
                    "duplicate": False,
                    "scheduled_at_us": 50_000 + position * 10_000,
                }
            )
    attempts.sort(
        key=lambda attempt: (
            attempt["scheduled_at_us"],
            attempt["logical_index"],
            attempt["duplicate"],
        )
    )
    return [attempt | {"attempt_index": index} for index, attempt in enumerate(attempts)]


def generate(
    directory: Path,
    seed=42,
    orders=100,
    scenario="steady",
    profile="normal",
    mode="fixed",
    group_id=None,
    schedule=None,
    configuration=None,
) -> dict:
    if mode not in {"fixed", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive"}:
        raise ValueError("Unsupported experiment mode")
    if group_id is not None and (not group_id.strip() or len(group_id) > 255):
        raise ValueError("Consumer group must be nonempty and at most 255 characters")
    run_id = new_run_id()
    if configuration is not None and not isinstance(configuration, dict):
        raise ValueError("Execution configuration must be a JSON object")
    attempts = logical_workload(seed, orders, scenario, profile, run_id, schedule)
    directory.mkdir(parents=True, exist_ok=False)
    with (directory / "manifest.jsonl").open("w", encoding="utf-8") as stream:
        for attempt in attempts:
            stream.write(json.dumps(attempt, sort_keys=True) + "\n")
    run = {
        "artifact_version": 1,
        "generator_version": 2,
        "run_id": str(run_id),
        "group_id": group_id or f"streamscale.run.{run_id}",
        "seed": seed,
        "orders": orders,
        "scenario": scenario,
        "profile": profile,
        "mode": mode,
        "publishing_schedule": schedule or SCENARIOS[scenario],
        "experiment_configuration": configuration or {},
        "created_at": utc_now(),
        "status": "generated",
        "attempts": len(attempts),
        "valid_unique": len(
            {a["event_id"] for a in attempts if not a["fault"].startswith("invalid_")}
        ),
        "invalid_attempts": sum(a["fault"].startswith("invalid_") for a in attempts),
        "duplicate_attempts": sum(a["duplicate"] for a in attempts),
        "manifest_sha256": manifest_hash(attempts),
        "logical_sha256": manifest_hash(attempts, logical=True),
    }
    write_json(directory / "run.json", run)
    return run


def wire_record(
    attempt: dict, run_id: str, anchor: datetime, produced_at: str
) -> tuple[bytes, bytes]:
    key = attempt["order_id"].encode("ascii")
    event = {
        "run_id": run_id,
        "event_id": attempt["event_id"],
        "order_id": attempt["order_id"],
        "event_type": attempt["event_type"],
        "event_time": (anchor + timedelta(microseconds=attempt["event_time_us"]))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        "produced_at": produced_at,
        "order_amount": attempt["order_amount"],
        "schema_version": 1,
    }
    if attempt["fault"] == "invalid_amount":
        event["order_amount"] = "-1.00"
    payload = json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
    if attempt["fault"] == "invalid_json":
        payload = b'{"broken":'
    elif attempt["fault"] == "invalid_encoding":
        payload = b"\xff\x00"
    elif attempt["fault"] == "invalid_key":
        key = b"deliberately-wrong-key"
    timestamp(produced_at)
    return key, payload


def encoded(value: bytes | None) -> str | None:
    return None if value is None else base64.b64encode(value).decode("ascii")

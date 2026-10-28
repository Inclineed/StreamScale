"""Explicit measurement windows, empirical percentiles and sampled resource integrals."""

import math
import re
from datetime import UTC, datetime

from streamscale.contract import timestamp


def epoch(value):
    return timestamp(value).timestamp()


def nearest_rank(values, fraction=0.95):
    if not 0 < fraction <= 1:
        raise ValueError("Percentile fraction must be in (0, 1]")
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


def quantity(value, resource):
    match = re.fullmatch(r"([0-9]+(?:[.][0-9]+)?)([a-zA-Z]*)", value)
    if not match:
        raise ValueError("Unsupported Kubernetes resource quantity")
    factors = (
        {"": 1, "n": 1e-9, "u": 1e-6, "m": 1e-3}
        if resource == "cpu"
        else {
            "": 1,
            "Ki": 1024,
            "Mi": 1024**2,
            "Gi": 1024**3,
            "K": 1000,
            "M": 1000**2,
            "G": 1000**3,
        }
    )
    if match[2] not in factors:
        raise ValueError("Unsupported Kubernetes resource unit")
    return float(match[1]) * factors[match[2]]


def integral(samples, field, start, end):
    """Left-step integral over observed coverage, without inventing missing leading data."""
    points = sorted(
        (sample["time"], sample[field])
        for sample in samples
        if sample.get(field) is not None and sample["time"] <= end
    )
    total = coverage = 0.0
    for index, (at, value) in enumerate(points):
        until = points[index + 1][0] if index + 1 < len(points) else end
        seconds = max(0.0, min(end, until) - max(start, at))
        total += value * seconds
        coverage += seconds
    return total, coverage


def recovery(samples, transition, interval=1):
    zeros = []
    previous = None
    for sample in samples:
        if sample["time"] < transition or sample.get("lag") is None:
            continue
        fresh = sample.get("exporter_sample_at")
        if fresh is None:
            continue
        if previous is not None and fresh <= previous:
            continue
        previous = fresh
        if sum(sample["lag"].values()) != 0:
            zeros.clear()
            continue
        if zeros and sample["time"] - zeros[-1] > interval * 2:
            zeros.clear()
        zeros.append(sample["time"])
        if len(zeros) >= 5:
            return max(0, sample["time"] - transition)
    return None


def correct(inspection):
    return (
        inspection.get("status") == "passed"
        and not inspection.get("missing_valid_ids")
        and not inspection.get("unexpected_valid_ids")
        and inspection.get("duplicate_stored_ids") == 0
        and inspection.get("state_regressions") == 0
        and inspection.get("missing_dlq_sources") == 0
        and inspection.get("invalid_dlq_envelopes") == 0
        and not any(inspection.get("remaining_run_lag", {"missing": 1}).values())
    )


def summarize(run, measurements, samples, before, after, configuration):
    start = epoch(run["publish_started_at"])
    duration = sum(seconds for seconds, _ in run["publishing_schedule"])
    end = start + duration
    facts = measurements["facts"]
    times = [epoch(fact["ingested_at"]) for fact in facts]
    latencies = [epoch(fact["ingested_at"]) - epoch(fact["produced_at"]) for fact in facts]
    drained = (
        epoch(measurements["ended_at"]) if measurements["ended_at"] else max(times, default=start)
    )
    finish = max(end, drained, max((sample["time"] for sample in samples), default=start))
    visible = [sample for sample in samples if start <= sample["time"] <= finish]
    lag = [sample for sample in visible if sample.get("lag") is not None]
    reasons = []
    if not correct(before) or not correct(after):
        reasons.append("reconciliation or controlled-restart correctness failed")
    if len({fact["event_id"] for fact in facts}) != len(facts) or len(facts) != before.get(
        "stored_facts"
    ):
        reasons.append("raw SQL fact identities/count differ from reconciliation")
    if any(value < 0 for value in latencies):
        reasons.append("negative SQL latency indicates clock skew")
    window_lag = [sample for sample in lag if sample["time"] < end]
    coverage = min(1, len(window_lag) / (duration / configuration["sample_interval_seconds"]))
    if coverage < 0.8:
        reasons.append("less than 80% of planned measurement lag samples available")
    if not any(sample.get("cpu_cores") is not None for sample in visible):
        reasons.append("consumer resource measurements unavailable")
    if not facts:
        reasons.append("no accepted facts")
    replica_seconds, replica_coverage = integral(visible, "ready", start, finish)
    cpu_seconds, cpu_coverage = integral(visible, "cpu_cores", start, finish)
    transition = configuration.get("load_transition_seconds", 0)
    preceding = [
        sample for sample in visible if sample["time"] < start + transition and sample["ready"] > 0
    ]
    baseline = preceding[-1]["ready"] if preceding else 1
    responses = [
        sample["time"] - start - transition
        for sample in visible
        if sample["time"] >= start + transition and sample["ready"] > baseline
    ]
    recovery_seconds = (
        recovery(lag, start + configuration["recovery_transition_seconds"])
        if "recovery_transition_seconds" in configuration
        else None
    )
    processing_seconds = max(times) - min(times) if len(times) > 1 else 0
    row = {
        "run_id": run["run_id"],
        "mode": run["mode"],
        "scenario": run["scenario"],
        "repetition": configuration.get("repetition", 0),
        "phase": configuration["phase"],
        "status": "passed" if not reasons else "failed",
        "performance_eligible": int(not reasons),
        "failure_reason": "; ".join(reasons),
        "seed": run["seed"],
        "logical_sha256": run["logical_sha256"],
        "image": run["image"],
        "git_commit": run["git_commit"],
        "git_dirty": run["git_dirty"],
        "measurement_start": run["publish_started_at"],
        "measurement_end": datetime.fromtimestamp(end, UTC).isoformat(),
        "database_drained_at": measurements["ended_at"],
        "sampling_ended_at": datetime.fromtimestamp(finish, UTC).isoformat(),
        "measurement_seconds": duration,
        "publish_seconds": epoch(run["publish_ended_at"]) - start,
        "drain_inclusive_seconds": max(0, finish - start),
        "acknowledged": run["acknowledged"],
        "unique_facts": len(facts),
        "duplicate_attempts": before["duplicate_attempts"],
        "invalid_attempts": before["invalid_attempts"],
        "dlq_copies": before["dlq_copies"],
        "dlq_unique_sources": before["dlq_unique_sources"],
        "throughput_window_per_second": sum(start <= at < end for at in times) / duration,
        "throughput_processing_per_second": len(facts) / processing_seconds
        if processing_seconds
        else None,
        "throughput_drain_inclusive_per_second": len(facts) / (finish - start)
        if finish > start
        else None,
        "p95_end_to_end_seconds": nearest_rank(latencies),
        "maximum_sampled_lag": max((sum(sample["lag"].values()) for sample in lag), default=None),
        "recovery_seconds": recovery_seconds,
        "recovery_censored": int(
            "recovery_transition_seconds" in configuration and recovery_seconds is None
        ),
        "scaling_response_seconds": min(responses) if responses else None,
        "max_ready_replicas": max((sample["ready"] for sample in visible), default=0),
        "replica_seconds": replica_seconds,
        "replica_coverage_seconds": replica_coverage,
        "cpu_core_seconds": cpu_seconds,
        "cpu_coverage_seconds": cpu_coverage,
        "average_cpu_cores": cpu_seconds / cpu_coverage if cpu_coverage else None,
        "peak_memory_bytes": max(
            (
                sample["memory_bytes"]
                for sample in visible
                if sample.get("memory_bytes") is not None
            ),
            default=None,
        ),
        "lag_sample_coverage": coverage,
        "controlled_restart_passed": int(correct(after)),
        "protocol_version": configuration["protocol_version"],
    }
    if configuration["protocol_version"] >= 3:
        row.update(sale_metrics(visible, start, configuration.get("sale_starts", [])))
        row["shape_seed"] = configuration.get("shape_seed")
    return row


def sale_metrics(samples, start, sale_starts):
    """Protocol v3: readiness at each sale's start, per-sale response and scale changes."""
    ordered = sorted(samples, key=lambda sample: sample["time"])
    desired = [sample["desired"] for sample in ordered if sample.get("desired") is not None]
    changes = sum(1 for before, after in zip(desired, desired[1:], strict=False) if after != before)
    ready_at, responses = [], []
    bounds = [*sale_starts[1:], None] if sale_starts else []
    for offset, following in zip(sale_starts, bounds, strict=True):
        moment = start + offset
        until = None if following is None else start + following
        before = [sample for sample in ordered if sample["time"] <= moment]
        if not before:
            continue
        initial = before[-1]["ready"]
        ready_at.append(initial)
        rises = [
            sample["time"] - moment
            for sample in ordered
            if sample["time"] > moment
            and (until is None or sample["time"] < until)
            and sample["ready"] > initial
        ]
        if initial < 3 and rises:
            responses.append(min(rises))
    return {
        "sales": len(sale_starts),
        "mean_ready_at_sale_start": sum(ready_at) / len(ready_at) if ready_at else None,
        "min_ready_at_sale_start": min(ready_at) if ready_at else None,
        "mean_sale_response_seconds": sum(responses) / len(responses) if responses else None,
        "scale_changes": changes,
    }

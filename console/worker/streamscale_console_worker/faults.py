"""Failure lab: deliberate, bounded faults with supervised automatic recovery.

The agent owns every fault's timer and recovery, persisted on the runs volume, so
closing the browser, stopping the console server or restarting the agent never
leaves a fault unrecovered. Each action names its scope, and every outcome is
observed (exit codes, claim records, SQL/DLQ checks) rather than assumed.

Consumer-side faults (processing delay, crash after a DB commit, crash after a
DLQ acknowledgement, memory exhaustion) are directives the console consumers
read from /runs/console/control/faults.json. Platform faults use the agent's
scoped Kubernetes permissions: deleting a console consumer pod, resizing CPU in
place, scaling console consumers to zero, and briefly scaling the Kafka or
PostgreSQL StatefulSet to zero.
"""

import json
import os
import threading
import time
from uuid import UUID

import psycopg

from streamscale.run_artifacts import utc_now, write_json
from streamscale_console_worker import control
from streamscale_console_worker.kube import CONSUMER_APP, POSTGRES_POD
from streamscale_console_worker.scaling import CONTROLLED

CATALOG = {
    "restart_consumer": {
        "label": "Restart a consumer",
        "scope": "Deletes one console consumer pod; its Deployment replaces it.",
        "temporary": False,
    },
    "crash_after_db_commit": {
        "label": "Crash after a DB commit",
        "scope": (
            "One console consumer exits (code 75) after PostgreSQL commits a record, before "
            "its offset commit."
        ),
        "temporary": False,
    },
    "crash_after_dlq_ack": {
        "label": "Crash after a DLQ acknowledgement",
        "scope": (
            "One console consumer exits (code 76) after the DLQ acknowledges an invalid "
            "record, before its offset commit."
        ),
        "temporary": False,
    },
    "stop_consumers": {
        "label": "Stop all consumers",
        "scope": (
            "Sets console consumers to zero replicas (manual override), then restores the "
            "scaling mode."
        ),
        "temporary": True,
    },
    "slow_processing": {
        "label": "Slow processing",
        "scope": "Console consumers wait before each database write.",
        "temporary": True,
    },
    "restrict_cpu": {
        "label": "Restrict consumer CPU",
        "scope": "Lowers console consumers' CPU limit in place (no restart), then restores it.",
        "temporary": True,
    },
    "restrict_db_cpu": {
        "label": "Restrict database CPU",
        "scope": "Lowers the shared PostgreSQL pod's CPU limit in place, then restores it.",
        "temporary": True,
    },
    "postgres_outage": {
        "label": "PostgreSQL outage",
        "scope": (
            "Scales the shared PostgreSQL StatefulSet to zero, then back to one. Data is retained."
        ),
        "temporary": True,
    },
    "kafka_outage": {
        "label": "Kafka outage",
        "scope": (
            "Scales the shared Kafka StatefulSet to zero, then back to one. Records are retained."
        ),
        "temporary": True,
    },
    "memory_pressure": {
        "label": "Memory exhaustion (OOM)",
        "scope": "One console consumer allocates memory until its 256Mi limit kills it (bounded).",
        "temporary": False,
    },
}
CONSUMER_DIRECTIVES = {
    "slow_processing",
    "crash_after_db_commit",
    "crash_after_dlq_ack",
    "memory_pressure",
}
EXCLUSIVE = {"postgres_outage", "kafka_outage"}
ORIGINAL_CPU = {"consumer": ("500m", "100m"), "postgres": ("500m", "100m")}
ONE_SHOT_SECONDS = 120


class FaultError(ValueError):
    pass


def _int(params, key, low, high, default):
    value = params.get(key, default)
    if type(value) is not int or not low <= value <= high:
        raise FaultError(f"{key} must be an integer from {low} to {high}")
    return value


def validate(kind, params):
    if kind not in CATALOG:
        raise FaultError("Unknown fault")
    params = params or {}
    clean = {}
    if kind in {"stop_consumers", "slow_processing", "restrict_cpu", "restrict_db_cpu"}:
        clean["duration_s"] = _int(params, "duration_s", 5, 300, 30)
    if kind in EXCLUSIVE:
        clean["duration_s"] = _int(params, "duration_s", 5, 120, 20)
    if kind == "slow_processing":
        clean["delay_ms"] = _int(params, "delay_ms", 5, 2000, 50)
    if kind in {"restrict_cpu", "restrict_db_cpu"}:
        clean["limit_m"] = _int(params, "limit_m", 20, 400, 100 if kind == "restrict_cpu" else 50)
    if kind in {
        "restart_consumer",
        "crash_after_db_commit",
        "crash_after_dlq_ack",
        "memory_pressure",
    }:
        pod = params.get("pod")
        if pod is not None and (not isinstance(pod, str) or not pod.startswith(CONSUMER_APP)):
            raise FaultError("Choose a console consumer pod")
        clean["pod"] = pod
    if kind == "restart_consumer" and not clean["pod"]:
        raise FaultError("Choose which consumer to restart")
    return clean


class FaultSupervisor:
    def __init__(self, agent, root=control.ROOT):
        self.agent = agent
        self.kube = agent.kube
        self.root = root
        self.lock = threading.RLock()
        self.path = root / "control" / "faults-state.json"
        self.state = control.read_json(self.path) or {"active": [], "history": []}
        self.commands = {
            "fault.inject": self.inject,
            "fault.recover": self.recover_command,
            "fault.recover_all": self.recover_all,
        }
        self.sections = {"faults": self.section}
        self.runners = [self.supervise]
        self.busy = [lambda: bool(self.state["active"])]

    # Persistence -----------------------------------------------------------------

    def save(self):
        with self.lock:
            snapshot = json.loads(json.dumps(self.state))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        write_json(self.path, snapshot)
        directives = [
            {key: fault[key] for key in ("id", "kind", "pod", "params", "expires_at")}
            for fault in snapshot["active"]
            if fault["kind"] in CONSUMER_DIRECTIVES
        ]
        control.write_faults(directives, self.root)

    # Commands ----------------------------------------------------------------------

    def inject(self, body):
        kind = body.get("kind")
        params = validate(kind, body.get("params"))
        with self.lock:
            active = {fault["kind"] for fault in self.state["active"]}
            if kind in active:
                raise FaultError(f"{CATALOG[kind]['label']} is already active")
            if kind in EXCLUSIVE and active & EXCLUSIVE:
                raise FaultError("Only one infrastructure outage at a time")
            fault = {
                "id": "fault-" + os.urandom(5).hex(),
                "kind": kind,
                "label": CATALOG[kind]["label"],
                "scope": CATALOG[kind]["scope"],
                "params": params,
                "pod": params.get("pod"),
                "started_at": time.time(),
                "expires_at": time.time() + params.get("duration_s", ONE_SHOT_SECONDS),
                "state": "active",
                "events": [],
                "run_id": (self.agent.observer.context.run or {}).get("run_id"),
            }
            self.begin(fault)
            self.state["active"].append(fault)
        self.save()
        return {"id": fault["id"], "kind": kind, "expires_at": fault["expires_at"]}

    def begin(self, fault):
        kind, params = fault["kind"], fault["params"]
        if kind == "restart_consumer":
            self.kube.delete_console_pod(params["pod"])
            fault["events"].append(_event(f"Deleted pod {params['pod']}"))
            fault["expires_at"] = time.time() + 90
        elif kind == "stop_consumers":
            scaling = self.agent.components.get("scaling")
            if scaling is None:
                raise FaultError("Scaling control is unavailable")
            fault["previous"] = {
                "mode": scaling.state["mode"],
                "override": scaling.state["override"],
                "manual_replicas": scaling.state["manual_replicas"],
            }
            if scaling.state["mode"] in CONTROLLED:
                scaling.enter_override({"replicas": 0})
            else:
                scaling.set_mode({"mode": "manual", "replicas": 0})
            fault["events"].append(_event("Console consumers set to zero replicas"))
        elif kind == "restrict_cpu":
            self.restrict_consumers(fault)
        elif kind == "restrict_db_cpu":
            self.kube.resize_cpu(POSTGRES_POD, f"{params['limit_m']}m")
            fault["events"].append(_event(f"PostgreSQL CPU limit set to {params['limit_m']}m"))
        elif kind == "postgres_outage":
            self.kube.scale_infrastructure("postgres", 0)
            fault["events"].append(_event("PostgreSQL scaled to zero"))
        elif kind == "kafka_outage":
            self.kube.scale_infrastructure("kafka", 0)
            fault["events"].append(_event("Kafka scaled to zero"))
        else:
            fault["events"].append(_event("Directive sent to console consumers"))

    def restrict_consumers(self, fault):
        limit = f"{fault['params']['limit_m']}m"
        restricted = set(fault.get("restricted", []))
        for pod in self.kube.console_pods():
            name = pod["metadata"]["name"]
            if name in restricted or pod["metadata"].get("deletionTimestamp"):
                continue
            if pod.get("status", {}).get("phase") != "Running":
                continue
            self.kube.resize_cpu(name, limit)
            restricted.add(name)
            fault["events"].append(_event(f"CPU limit of {name} set to {limit}"))
        fault["restricted"] = sorted(restricted)

    def recover_command(self, body):
        identifier = body.get("id")
        with self.lock:
            fault = next((row for row in self.state["active"] if row["id"] == identifier), None)
            if fault is None:
                raise FaultError("That fault is not active")
            self.finish(fault, "recovered on request")
        self.save()
        return {"id": identifier}

    def recover_all(self, body):
        with self.lock:
            identifiers = [fault["id"] for fault in self.state["active"]]
            for fault in list(self.state["active"]):
                self.finish(fault, "recovered with recover-all")
            # Defensive: make sure shared infrastructure is back even without a record.
            for role in ("postgres", "kafka"):
                if self.kube.statefulset(role)["spec"].get("replicas", 1) == 0:
                    self.kube.scale_infrastructure(role, 1)
        self.save()
        return {"recovered": identifiers}

    # Supervision -----------------------------------------------------------------

    def finish(self, fault, reason):
        kind = fault["kind"]
        try:
            if kind == "stop_consumers":
                previous = fault.get("previous") or {}
                scaling = self.agent.components.get("scaling")
                if scaling is not None:
                    self.restore_scaling(scaling, previous)
            elif kind == "restrict_cpu":
                limit, request = ORIGINAL_CPU["consumer"]
                for name in fault.get("restricted", []):
                    try:
                        self.kube.resize_cpu(name, limit, request)
                    except Exception:  # noqa: BLE001 - the pod may have been replaced
                        pass
            elif kind == "restrict_db_cpu":
                limit, request = ORIGINAL_CPU["postgres"]
                self.kube.resize_cpu(POSTGRES_POD, limit, request)
            elif kind == "postgres_outage":
                self.kube.scale_infrastructure("postgres", 1)
            elif kind == "kafka_outage":
                self.kube.scale_infrastructure("kafka", 1)
        except Exception as error:  # noqa: BLE001 - retried by the supervisor
            fault["events"].append(_event(f"Recovery attempt failed: {type(error).__name__}"))
            fault["state"] = "recovering"
            return False
        fault["state"] = "recovered"
        fault["ended_at"] = time.time()
        fault["events"].append(_event(f"Recovered: {reason}"))
        self.state["active"] = [row for row in self.state["active"] if row["id"] != fault["id"]]
        self.state["history"] = (self.state["history"] + [fault])[-40:]
        return True

    def restore_scaling(self, scaling, previous):
        deadline = time.monotonic() + 120
        while scaling.state["transition"]["state"] == "running" and time.monotonic() < deadline:
            time.sleep(0.5)
        if previous.get("mode") in CONTROLLED and not previous.get("override"):
            if scaling.state["override"]:
                scaling.release_override({})
            else:
                scaling.set_mode({"mode": previous["mode"]})
        elif previous.get("override"):
            scaling.set_replicas({"replicas": previous["override"]["replicas"]})
        else:
            scaling.set_mode(
                {
                    "mode": previous.get("mode") or "fixed",
                    "replicas": previous.get("manual_replicas", 1),
                }
            )

    def supervise(self, stop):
        # On start, faults recorded before an agent restart continue to their deadline.
        while not stop.wait(0.5):
            changed = False
            with self.lock:
                for fault in list(self.state["active"]):
                    try:
                        changed |= self.tick(fault)
                    except Exception as error:  # noqa: BLE001
                        fault["events"].append(_event(f"Supervisor error: {type(error).__name__}"))
                        changed = True
            if changed:
                self.save()

    def tick(self, fault):
        kind = fault["kind"]
        now = time.time()
        if kind in {"crash_after_db_commit", "crash_after_dlq_ack", "memory_pressure"}:
            claim = control.read_claim(fault["id"], self.root)
            if claim and "claim" not in fault:
                fault["claim"] = claim
                fault["events"].append(_event(_claim_text(kind, claim)))
                fault["state"] = "fired"
                return True
            if "claim" in fault:
                return self.verify_replay(fault) or self.maybe_close(fault, now)
        if kind == "restart_consumer" and now >= fault["expires_at"]:
            return self.finish(fault, "replacement pod observed")
        if kind == "restrict_cpu" and now < fault["expires_at"]:
            before = len(fault.get("restricted", []))
            self.restrict_consumers(fault)
            if len(fault["restricted"]) != before:
                return True
        if now >= fault["expires_at"]:
            if kind in {"crash_after_db_commit", "crash_after_dlq_ack", "memory_pressure"}:
                if "claim" not in fault:
                    fault["events"].append(_event("Expired: no matching record was processed"))
                return self.finish(fault, "one-shot window closed")
            return self.finish(fault, "duration elapsed")
        return False

    def maybe_close(self, fault, now):
        if fault.get("verified") is not None or now >= fault["expires_at"]:
            return self.finish(fault, "outcome recorded")
        return False

    def verify_replay(self, fault):
        """After a crash, the replacement must replay the record without a second fact."""
        if fault.get("verified") is not None:
            return False
        claim = fault["claim"]
        if fault["kind"] == "memory_pressure":
            return self.verify_oom(fault)
        run = self.agent.observer.context.run
        latest = self.agent.observer.latest() or {}
        partitions = ((latest.get("kafka") or {}).get("partitions")) or []
        row = next((item for item in partitions if item["partition"] == claim["partition"]), None)
        if not row or row.get("committed") is None or row["committed"] <= claim["offset"]:
            return False  # not yet replayed and committed past the crash point
        if fault["kind"] == "crash_after_db_commit":
            with psycopg.connect(self.agent.settings.database_url, connect_timeout=5) as connection:
                stored = connection.execute(
                    "SELECT count(*) FROM order_event WHERE event_id = %s AND run_id = %s",
                    (UUID(claim["event_id"]), UUID(run["run_id"])),
                ).fetchone()[0]
            fault["verified"] = stored == 1
            fault["events"].append(
                _event(
                    f"Offset committed past {claim['partition']}/{claim['offset']} after replay; "
                    f"event stored {stored} time(s)"
                )
            )
        else:
            copies = sum(
                1
                for envelope in self.agent.components["inspection"].dlq.summary()["recent"]
                if envelope.get("source", {}).get("partition") == claim["partition"]
                and envelope.get("source", {}).get("offset") == claim["offset"]
            )
            fault["verified"] = True
            fault["dlq_copies_seen"] = copies
            fault["events"].append(
                _event(
                    f"Offset committed past {claim['partition']}/{claim['offset']}; "
                    f"{copies} DLQ cop{'y' if copies == 1 else 'ies'} of that source seen recently"
                )
            )
        return True

    def verify_oom(self, fault):
        pod = fault["claim"].get("pod")
        try:
            status = self.kube.pod(pod)["status"]
        except Exception:  # noqa: BLE001 - pod replaced; outcome from events below
            return False
        container = (status.get("containerStatuses") or [{}])[0]
        last = container.get("lastState", {}).get("terminated") or {}
        if last.get("reason") == "OOMKilled":
            fault["verified"] = True
            fault["events"].append(
                _event(f"{pod} was OOMKilled (exit {last.get('exitCode')}) and restarted")
            )
            return True
        return False

    def section(self):
        with self.lock:
            state = json.loads(json.dumps(self.state))
        return {
            "active": state["active"],
            "history": state["history"][-12:],
            "catalog": CATALOG,
            "now": time.time(),
        }


def _claim_text(kind, claim):
    if kind == "crash_after_db_commit":
        return (
            f"{claim['pod']} exited 75 after storing event {claim['event_id'][:8]} "
            f"({claim['partition']}/{claim['offset']}) and before committing its offset"
        )
    if kind == "crash_after_dlq_ack":
        return (
            f"{claim['pod']} exited 76 after the DLQ acknowledged {claim['error_code']} from "
            f"{claim['partition']}/{claim['offset']} and before committing its offset"
        )
    return f"{claim['pod']} began allocating memory toward its limit"


def _event(text):
    return {"at": time.time(), "text": text, "iso": utc_now()}


def component(agent):
    return FaultSupervisor(agent)

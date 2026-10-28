"""Sale presets, sale operations and per-session records on the host.

A session is one interactive sale. Its folder under artifacts/console/sessions/
holds the configuration, an action journal and the derived telemetry recorded on
the console's clock, so sessions can be replayed and compared later. The console
holds the cluster workflow lock from sale start until the sale has drained.
"""

import asyncio
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from fastapi import Request
from fastapi.responses import JSONResponse

from streamscale_console.cluster import ARTIFACTS, ROOT
from streamscale_console.operations import Rejected
from streamscale_console.telemetry import AgentRejected, AgentUnavailable
from streamscale_console.workflow import WorkflowBusy

PROTOCOL = ROOT / "docs/results/current/frozen-protocol.json"
SESSIONS = ARTIFACTS / "sessions"
ACTIVE_SALE = {"preparing", "publishing", "paused", "stopping"}
DEFAULT_FAULTS = {"malformed_pct": 1.0, "duplicate_pct": 2.0, "late_pct": 5.0}
QUERIES = {"facts.recent", "order", "event", "dlq.recent"}


def presets(protocol_path=PROTOCOL):
    """Presets reuse the frozen M7 rates and seeds, so workloads match the measured ones."""
    try:
        protocol = json.loads(protocol_path.read_text())
        rates = protocol["rates"]
        scenarios = protocol["scenarios"]
        capacity = protocol.get("pilot_capacity_per_second")
    except (OSError, KeyError, json.JSONDecodeError):
        rates = {"steady": 52, "surge": 390}
        scenarios = {
            "steady": {"seed": 101, "schedule": [[60, 52]]},
            "flash-sale": {"seed": 202, "schedule": [[15, 52], [20, 390], [25, 52]]},
            "overload-recovery": {"seed": 303, "schedule": [[35, 390], [25, 52]]},
        }
        capacity = None
    steady, surge = rates["steady"], rates["surge"]
    rows = [
        {
            "id": "flash-sale",
            "label": "Flash sale",
            "scenario": "flash-sale",
            "seed": scenarios["flash-sale"]["seed"],
            "schedule": scenarios["flash-sale"]["schedule"],
            "description": f"Quiet, a {surge}/s rush, then quiet again (the measured scenario).",
        },
        {
            "id": "flash-sale-extended",
            "label": "Extended flash sale",
            "scenario": "flash-sale",
            "seed": scenarios["flash-sale"]["seed"],
            "schedule": [[20, steady], [60, surge], [40, steady]],
            "description": "A one-minute rush that leaves time to change scaling during a demo.",
        },
        {
            "id": "steady",
            "label": "Steady",
            "scenario": "steady",
            "seed": scenarios["steady"]["seed"],
            "schedule": scenarios["steady"]["schedule"],
            "description": f"{steady}/s for a minute: well within one consumer's capacity.",
        },
        {
            "id": "overload-recovery",
            "label": "Overload and recovery",
            "scenario": "overload-recovery",
            "seed": scenarios["overload-recovery"]["seed"],
            "schedule": scenarios["overload-recovery"]["schedule"],
            "description": "Starts overloaded, then drops to steady traffic to show recovery.",
        },
        *forecastable(steady, surge),
    ]
    for row in rows:
        row["faults"] = dict(DEFAULT_FAULTS)
        row["events"] = sum(seconds * rate for seconds, rate in row["schedule"])
        row["seconds"] = sum(seconds for seconds, _ in row["schedule"])
    return {"presets": rows, "rates": rates, "pilot_capacity_per_second": capacity}


MAX_PHASES = 12


def coarsen(phases, limit=MAX_PHASES):
    """Merge the closest-rate neighbours until a schedule fits the console's phase limit.

    The ramp's one-second climb becomes a few longer steps; rates stay even integers.
    """
    phases = [list(phase) for phase in phases]
    while len(phases) > limit:
        index = min(range(len(phases) - 1), key=lambda i: abs(phases[i][1] - phases[i + 1][1]))
        (first, a), (second, b) = phases[index], phases[index + 1]
        rate = max(2, round((first * a + second * b) / (first + second) / 2) * 2)
        phases[index : index + 2] = [[first + second, rate]]
    return phases


def forecastable(steady, surge):
    """M16 shapes where forecasting can help: a gradual ramp and repeated sales.

    They follow the measured v3 repetition-1 shapes (the ramp coarsened to at most 12
    phases), so a console demo resembles the campaign.
    """
    from streamscale_workload.shapes import sale_starts, schedule

    rows = []
    for identifier, label, seed, text in (
        (
            "ramp",
            "Gradual ramp",
            404,
            "Traffic climbs over about 20 s to near the rush rate; a trend forecast helps.",
        ),
        (
            "repeated-sales",
            "Repeated sales",
            505,
            "Several seeded rushes about half a minute apart; a forecaster learns the rhythm.",
        ),
    ):
        phases = coarsen(schedule(identifier, f"eval:{seed * 100 + 1}", steady, surge))
        rows.append(
            {
                "id": identifier,
                "label": label,
                "scenario": identifier,
                "seed": seed * 10 + 1,
                "schedule": phases,
                "description": f"{text} {len(sale_starts(phases, steady))} sale(s).",
            }
        )
    return rows


def now_text():
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


class SessionStore:
    def __init__(self, directory=SESSIONS):
        self.directory = directory

    @staticmethod
    def new_id():
        return "session-" + datetime.now(UTC).strftime("%Y%m%d%H%M%S") + "-" + os.urandom(3).hex()

    def create(self, config, sale, identifier=None):
        identifier = identifier or self.new_id()
        record = {
            "id": identifier,
            "created_at": now_text(),
            "config": config,
            "run_name": sale["run_name"],
            "run_id": sale["run_id"],
            "group_id": sale["group_id"],
            "attempts": sale["attempts"],
            "state": "publishing",
            "interactive": True,
            "segments": [],
        }
        self.save(record)
        return record

    def path(self, identifier):
        if not identifier.startswith("session-") or "/" in identifier or ".." in identifier:
            raise Rejected("Unknown session")
        return self.directory / identifier

    def save(self, record):
        folder = self.path(record["id"])
        folder.mkdir(parents=True, exist_ok=True)
        temporary = folder / ".session.json.tmp"
        temporary.write_text(json.dumps(record, indent=2, default=str) + "\n")
        temporary.replace(folder / "session.json")

    def load(self, identifier):
        path = self.path(identifier) / "session.json"
        if not path.exists():
            raise Rejected("Unknown session")
        return json.loads(path.read_text())

    def find_run(self, run_name):
        if not self.directory.exists():
            return None
        for folder in sorted(self.directory.iterdir(), reverse=True):
            path = folder / "session.json"
            if path.exists():
                record = json.loads(path.read_text())
                if record.get("run_name") == run_name:
                    return record
        return None

    def list(self, limit=100):
        if not self.directory.exists():
            return []
        rows = []
        for folder in sorted(self.directory.iterdir(), reverse=True)[:limit]:
            path = folder / "session.json"
            if path.exists():
                record = json.loads(path.read_text())
                rows.append({key: record.get(key) for key in SUMMARY_KEYS})
        return rows


SUMMARY_KEYS = (
    "id",
    "created_at",
    "ended_at",
    "state",
    "outcome",
    "run_name",
    "run_id",
    "config",
    "reconciliation_status",
    "reconciliation",
    "comparable",
    "segments",
    "faults",
)


class Component:
    def __init__(self, console):
        self.console = console
        self.store = SessionStore()
        self.record = None
        self.telemetry = None
        self.drained_at = None
        self.cleaned = False
        self.catalog = presets()
        console.hub.sample_listeners.append(self.observe)
        coordinator = console.coordinator
        coordinator.register("sale.start", self.start, "Start sale", check=self.check_start)
        for name, label in (
            ("sale.pause", "Pause new shoppers"),
            ("sale.resume", "Resume the sale"),
            ("sale.stop", "End the sale and drain"),
            ("sale.abort", "Abort the sale"),
        ):
            coordinator.register(name, self.forward(name), label, check=self.check_active)
        coordinator.register("sale.rate", self.rate, "Change arrival rate", check=self.check_active)
        coordinator.register("session.reconcile", self.reconcile, "Reconcile sale", mutating=False)
        coordinator.register("preset.save", self.save_preset, "Save sale preset", mutating=False)
        coordinator.register(
            "preset.delete", self.delete_preset, "Delete sale preset", mutating=False
        )

    # State shared with the browser -------------------------------------------

    def snapshot(self):
        return {
            "sales": {
                "presets": self.catalog,
                "saved": self.saved_presets(),
                "session": self.record,
                "sessions": self.store.list(30),
            }
        }

    def tasks(self):
        return []

    def routes(self, app):
        @app.get("/api/inspect/{name}")
        async def inspect(name: str, request: Request):
            if name not in QUERIES:
                return JSONResponse({"error": "Unknown inspection"}, status_code=404)
            parameters = {
                key: value
                for key, value in request.query_params.items()
                if key in {"run", "order_id", "event_id", "limit"}
            }
            query = "&".join(f"{key}={value}" for key, value in parameters.items())
            try:
                return await self.console.hub.client.call(
                    "GET", f"/v1/query/{name}" + (f"?{query}" if query else ""), timeout=10
                )
            except AgentRejected as error:
                return JSONResponse({"error": str(error)}, status_code=error.status)
            except AgentUnavailable as error:
                return JSONResponse({"error": f"Agent unavailable: {error}"}, status_code=503)

        @app.get("/api/sessions")
        async def sessions():
            return {"sessions": self.store.list()}

    # Operations ----------------------------------------------------------------

    def sale_status(self):
        sample = self.console.hub.latest or {}
        return sample.get("sale") or {}

    def check_start(self, params):
        if self.sale_status().get("state") in ACTIVE_SALE:
            raise Rejected("A sale is already running; end or abort it first")
        if self.record is not None:
            # The session record changes immediately, unlike the next telemetry sample.
            if self.record.get("state") == "draining":
                # A new run would move consumers off the old range and strand its backlog.
                raise Rejected(
                    "The previous sale is still draining; wait until its backlog is zero"
                )
            raise Rejected("A sale is already running; end or abort it first")
        if self.console.hub.status["agent"] != "connected":
            raise Rejected("The cluster agent is not connected")

    def check_active(self, params):
        publishing = self.record is not None and self.record.get("state") == "publishing"
        # The session record updates at once; telemetry can lag the start by a sample.
        if not publishing and self.sale_status().get("state") not in ACTIVE_SALE:
            raise Rejected("No sale is running")

    def build_config(self, params):
        preset_id = params.get("preset", "flash-sale")
        preset = next((row for row in self.catalog["presets"] if row["id"] == preset_id), None)
        if preset is None:
            preset = next((row for row in self.saved_presets() if row["id"] == preset_id), None)
        if preset is None and preset_id != "custom":
            raise Rejected("Unknown preset")
        base = preset or {"scenario": "flash-sale", "seed": 202, "schedule": [], "faults": {}}
        config = {
            "preset": preset_id,
            "scenario": params.get("scenario", base["scenario"]),
            "seed": params.get("seed", base["seed"]),
            "schedule": params.get("schedule", base["schedule"]),
            "faults": params.get("faults", base["faults"]),
            "mode": params.get("mode") or self.current_mode(),
        }
        return config

    def current_mode(self):
        scaling = (self.console.hub.latest or {}).get("scaling") or {}
        return scaling.get("mode") or "fixed"

    async def start(self, operation):
        config = self.build_config(operation.params)
        identifier = self.store.new_id()
        config["session"] = identifier
        try:
            self.console.lock.acquire("sale")
        except WorkflowBusy as error:
            raise Rejected(str(error)) from None
        try:
            self.console.coordinator.report(operation, "Generating the seeded workload")
            response = await self.console.hub.client.call(
                "POST", "/v1/command/sale.start", {"config": config}, timeout=60
            )
        except AgentRejected as error:
            self.console.lock.release()
            raise Rejected(str(error)) from None
        except Exception:
            self.console.lock.release()
            raise
        sale = response["result"]
        record = self.store.create(config, sale, identifier)
        self.attach(record)
        operation.session = record["id"]
        self.console.journal.write(
            {
                "type": "system",
                "text": f"Sale {sale['run_name']} started: {sale['attempts']:,} events planned",
            },
            record["id"],
        )
        return sale | {"session": record["id"]}

    def forward(self, name):
        async def handler(operation):
            try:
                response = await self.console.hub.client.call(
                    "POST", f"/v1/command/{name}", operation.params, timeout=20
                )
            except AgentRejected as error:
                raise Rejected(str(error)) from None
            if name in {"sale.pause", "sale.resume", "sale.rate"} and self.record is not None:
                # Interactive pacing changes keep this session out of formal comparisons.
                self.record["controls_changed"] = True
                self.store.save(self.record)
            return response.get("result") or {}

        return handler

    async def rate(self, operation):
        rate = operation.params.get("rate")
        if rate is not None and (type(rate) is not int or not 1 <= rate <= 2000):
            raise Rejected("Rate must be 1–2000 events per second, or the schedule")
        return await self.forward("sale.rate")(operation)

    # Saved presets and reconciliation --------------------------------------------

    def presets_path(self):
        return ARTIFACTS / "presets.json"

    def saved_presets(self):
        path = self.presets_path()
        if not path.exists():
            return []
        try:
            return json.loads(path.read_text()).get("presets", [])
        except json.JSONDecodeError:
            return []

    async def save_preset(self, operation):
        params = operation.params
        name = str(params.get("label", "")).strip()[:60]
        if not name:
            raise Rejected("A saved preset needs a name")
        config = {key: params.get(key) for key in ("scenario", "seed", "schedule", "faults")}
        if config["scenario"] not in {"steady", "flash-sale", "overload-recovery"}:
            raise Rejected("Choose steady, flash-sale or overload-recovery as the scenario")
        schedule = config["schedule"]
        if not isinstance(schedule, list) or not schedule:
            raise Rejected("A preset needs at least one schedule phase")
        identifier = "saved-" + "".join(ch if ch.isalnum() else "-" for ch in name.lower())[:40]
        rows = [row for row in self.saved_presets() if row["id"] != identifier]
        rows.append(
            config
            | {
                "id": identifier,
                "label": name,
                "description": str(params.get("description", "Saved console preset"))[:160],
                "events": sum(int(seconds) * int(rate) for seconds, rate in schedule),
                "seconds": sum(int(seconds) for seconds, _ in schedule),
                "saved_at": now_text(),
            }
        )
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        self.presets_path().write_text(json.dumps({"presets": rows}, indent=2) + "\n")
        self.publish()
        return {"id": identifier}

    async def delete_preset(self, operation):
        identifier = operation.params.get("id")
        rows = [row for row in self.saved_presets() if row["id"] != identifier]
        self.presets_path().write_text(json.dumps({"presets": rows}, indent=2) + "\n")
        self.publish()
        return {"id": identifier}

    async def reconcile(self, operation):
        record = self.store.load(str(operation.params.get("session", "")))
        client = self.console.hub.client
        deadline = time.monotonic() + 120
        while self.console.hub.status["agent"] != "connected":
            # Queued at startup or during an agent restart: wait rather than fail.
            if time.monotonic() > deadline:
                raise RuntimeError("The cluster agent did not become reachable")
            await asyncio.sleep(1)
        try:
            await client.call("POST", "/v1/command/reconcile", {"run_name": record["run_name"]})
        except AgentRejected as error:
            raise Rejected(str(error)) from None
        self.console.coordinator.report(operation, "Reading the source range, SQL and DLQ")
        deadline = time.monotonic() + 600
        while True:
            job = await client.call(
                "GET", f"/v1/query/reconciliation?run={record['run_name']}", timeout=30
            )
            if job["state"] != "running":
                break
            if time.monotonic() > deadline:
                raise RuntimeError("Reconciliation did not finish within ten minutes")
            await asyncio.sleep(1)
        if job["state"] == "error":
            raise RuntimeError(job.get("error", "Reconciliation failed"))
        result = job["result"]
        folder = self.store.path(record["id"])
        (folder / "reconciliation.json").write_text(json.dumps(result, indent=2) + "\n")
        record = self.store.load(record["id"])
        record["reconciliation_status"] = result["status"]
        record["reconciliation"] = summarize_reconciliation(result)
        record["comparable"] = comparable(record, result)
        self.store.save(record)
        latency = result["latency_s"]["p95"]
        self.console.journal.write(
            {
                "type": "observation",
                "text": (
                    f"Reconciliation {result['status']} for {record['run_name']}: "
                    f"{result['stored_facts']:,} facts, "
                    f"{result['missing_acknowledged_count']} missing, "
                    f"{result['duplicate_stored_ids']} duplicates stored, "
                    f"{result['state_regressions']} state regressions"
                    + (f", exact SQL p95 {latency:.2f} s" if latency is not None else "")
                ),
            },
            record["id"],
        )
        self.publish()
        return record["reconciliation"]

    # Session lifecycle ------------------------------------------------------------

    def attach(self, record):
        self.record = record
        self.console.session = record["id"]
        self.drained_at = None
        folder = self.store.path(record["id"])
        self.telemetry = (folder / "telemetry.jsonl").open("a", encoding="utf-8")
        self.publish()

    def publish(self):
        self.console.publish(
            {"type": "component", "name": "sales", "value": self.snapshot()["sales"]}
        )

    def observe(self, sample, point, live=True):
        if not live:
            return
        sale = sample.get("sale") or {}
        if self.record is None:
            current = (sample.get("run") or {}).get("run_name")
            if not self.cleaned:
                self.cleaned = True
                self.close_orphans(current)
            if sale.get("state") in ACTIVE_SALE:
                self.recover(sale)
            elif current:
                record = self.store.find_run(current)
                if record and record.get("state") in {"publishing", "draining"}:
                    # The server restarted after publishing ended: finish draining here.
                    record["state"] = "draining"
                    record["sale_state"] = sale.get("state")
                    self.store.save(record)
                    try:
                        self.console.lock.acquire("sale drain (reattached)")
                    except WorkflowBusy:
                        record["lock_conflict"] = True
                    self.attach(record)
            return
        if self.telemetry is not None:
            frame = {"point": point, "sale": _sale_brief(sale), "sample": replay_sample(sample)}
            self.telemetry.write(json.dumps(frame, default=str) + "\n")
            self.telemetry.flush()
        recovered = sale.get("recovered") or {}
        if (
            sale.get("state") == "idle"
            and recovered.get("run_id") == self.record["run_id"]
            and self.record["state"] == "publishing"
        ):
            # The agent restarted mid-sale and finalized the run from its durable files.
            sale = {
                "run_name": self.record["run_name"],
                "state": "finished",
                "outcome": (recovered.get("console") or {}).get("outcome", "interrupted"),
                "acknowledged": recovered.get("acknowledged"),
                "attempted": (recovered.get("console") or {}).get("attempted"),
                "budget": recovered.get("attempts"),
            }
        state = sale.get("state")
        # Sale-state updates need the agent's sale to be this session's; the drain and
        # superseded checks below depend only on the run's offsets, so an idle agent
        # after a restart cannot strand the session.
        if sale.get("run_name") == self.record["run_name"] and state != self.record.get(
            "sale_state"
        ):
            self.record["sale_state"] = state
            if state in {"finished", "failed"}:
                self.record["outcome"] = sale.get("outcome")
                self.record["publish_ended_at"] = now_text()
                self.record["sale_summary"] = _sale_brief(sale)
                self.record["state"] = "draining"
            self.store.save(self.record)
            self.publish()
        current_run = (sample.get("run") or {}).get("run_name")
        if (
            self.record["state"] == "draining"
            and current_run
            and current_run != self.record["run_name"]
        ):
            # A newer run owns the consumers; this session can no longer drain here.
            self.record["superseded_by"] = current_run
            self.complete(state="superseded")
            return
        if self.record["state"] == "draining" and drained(sample, self.record["run_name"]):
            self.drained_at = self.drained_at or time.time()
            # Record a few seconds of the settled pipeline before closing the session.
            if time.time() - self.drained_at >= 5:
                self.complete()

    def complete(self, state="drained"):
        record = self.record
        record["state"] = state
        record["ended_at"] = now_text()
        self.store.save(record)
        self.console.journal.write(
            {
                "type": "system",
                "text": f"Sale {record['run_name']} drained; backlog is zero"
                if state == "drained"
                else f"Session for {record['run_name']} closed: superseded by a newer run",
            },
            record["id"],
        )
        if self.telemetry is not None:
            self.telemetry.close()
            self.telemetry = None
        self.console.lock.release()
        try:
            if state != "drained":
                raise Rejected("not drained")
            self.console.coordinator.submit(
                "session.reconcile", {"session": record["id"]}, "reconcile-" + record["id"]
            )
        except Rejected:
            pass
        for hook in getattr(self.console, "on_drained", []):
            asyncio.get_running_loop().create_task(hook(record))
        self.console.session = None
        self.record = None
        self.publish()

    def close_orphans(self, current):
        """Sessions left open by an earlier server whose run is no longer current."""
        if not self.store.directory.exists():
            return
        for folder in self.store.directory.iterdir():
            path = folder / "session.json"
            if not path.exists():
                continue
            record = json.loads(path.read_text())
            if record.get("state") == "drained" and not record.get("reconciliation_status"):
                # Drained, but the server stopped before its reconciliation ran.
                try:
                    self.console.coordinator.submit(
                        "session.reconcile", {"session": record["id"]}, "reconcile-" + record["id"]
                    )
                except Rejected:
                    pass
            if (
                record.get("state") in {"publishing", "draining"}
                and record.get("run_name") != current
            ):
                record["state"] = "orphaned"
                record["note"] = "The console server stopped before this sale drained"
                record["ended_at"] = record.get("ended_at") or now_text()
                self.store.save(record)

    def recover(self, sale):
        """The server restarted mid-sale: reattach to the agent's actual sale."""
        record = self.store.find_run(sale.get("run_name"))
        if record is None:
            record = {
                "id": self.store.new_id(),
                "created_at": now_text(),
                "config": sale.get("config"),
                "run_name": sale.get("run_name"),
                "run_id": sale.get("run_id"),
                "group_id": sale.get("group_id"),
                "attempts": sale.get("budget"),
                "state": "publishing",
                "interactive": True,
                "segments": [],
                "recovered": True,
            }
        record["reattached_at"] = now_text()
        self.store.save(record)
        try:
            self.console.lock.acquire("sale (reattached)")
        except WorkflowBusy:
            record["lock_conflict"] = True
        self.attach(record)
        self.console.journal.write(
            {"type": "system", "text": f"Reattached to running sale {record['run_name']}"},
            record["id"],
        )


def summarize_reconciliation(result):
    keys = (
        "status",
        "outcome",
        "acknowledged",
        "stored_facts",
        "missing_acknowledged_count",
        "duplicate_stored_ids",
        "state_regressions",
        "invalid_sources",
        "dlq_unique_sources",
        "dlq_copies",
        "missing_dlq_sources",
        "unacknowledged_in_log",
        "log_duplicates",
        "checks",
        "latency_s",
        "reconciled_at",
    )
    return {key: result.get(key) for key in keys}


def comparable(record, result):
    """Only complete, unmodified, reconciled sales may enter a formal comparison."""
    return (
        result["status"] == "passed"
        and result.get("outcome") == "completed"
        and not record.get("controls_changed")
        and not record.get("faults")
        and not record.get("segments")
    )


def drained(sample, run_name):
    run = sample.get("run") or {}
    if run.get("run_name") != run_name or run.get("status") != "published":
        return False
    partitions = (sample.get("kafka") or {}).get("partitions") or []
    return len(partitions) == 3 and all(row.get("run_remaining") == 0 for row in partitions)


def _sale_brief(sale):
    keys = (
        "state",
        "attempted",
        "acknowledged",
        "failed",
        "purged",
        "budget",
        "target_rate",
        "rate_mode",
        "paused",
        "outcome",
        "buffer_full",
    )
    return {key: sale.get(key) for key in keys}


def ensure(path: Path):
    path.mkdir(parents=True, exist_ok=True)
    return path


def replay_sample(sample):
    """The fields the floor needs to redraw a moment later, without bulky histograms."""
    kubernetes = sample.get("kubernetes") or {}
    return {
        "sampled_at": sample.get("sampled_at"),
        "sequence": sample.get("sequence"),
        "sources": sample.get("sources"),
        "run": sample.get("run"),
        "kafka": sample.get("kafka"),
        "sql": sample.get("sql"),
        "dlq": sample.get("dlq"),
        "kubernetes": {
            "consumer": kubernetes.get("consumer"),
            "pods": kubernetes.get("pods"),
            "controllers": kubernetes.get("controllers"),
            "infrastructure": kubernetes.get("infrastructure"),
            "resources": None,
        },
        "consumers": [
            {
                "pod": row.get("pod"),
                "ip": row.get("ip"),
                "ok": row.get("ok"),
                "state": row.get("state"),
            }
            for row in sample.get("consumers") or []
        ],
        "scaling": {
            key: (sample.get("scaling") or {}).get(key)
            for key in ("mode", "override", "ownership", "transition")
        },
        # The ML controller's latest decision, so replays can explain each scaling step.
        "autoscaler": {
            key: (sample.get("autoscaler") or {}).get(key) for key in ("status", "last", "policy")
        },
        "faults": {"active": ((sample.get("faults") or {}).get("active") or [])},
    }

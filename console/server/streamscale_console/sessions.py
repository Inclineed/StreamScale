"""Saved sessions: replay data, exports, formal comparisons and the measured matrix.

Replay reads recorded files only and can never mutate the pipeline. Formal
comparisons run fresh, sequential sales with frozen settings, one per mode, and
rank only sessions that completed unmodified and passed reconciliation. The M7
27-run results are served read-only from docs/results/current.
"""

import asyncio
import csv
import io
import json
import os
import time
from datetime import UTC, datetime

from fastapi.responses import FileResponse, JSONResponse, Response

from streamscale_console.cluster import ARTIFACTS, ROOT
from streamscale_console.operations import Rejected

RESULTS = ROOT / "docs/results/current"
COMPARISONS = ARTIFACTS / "comparisons"
FIGURES = {
    "primary-outcomes.svg",
    "resource-outcomes.svg",
    "trajectories.svg",
    "primary-outcomes.png",
    "resource-outcomes.png",
    "trajectories.png",
}
EXPORTS = {"points.csv", "configuration.json", "journal.jsonl", "reconciliation.json"}
POINT_COLUMNS = (
    ("t", lambda p: p["t"]),
    ("arrival_per_s", lambda p: p["rates"]["arrival"]),
    ("served_per_s", lambda p: p["rates"]["drain"]),
    ("stored_per_s", lambda p: p["rates"]["accepted_sql"]),
    ("lag_0", lambda p: (p["lag"] or [None] * 3)[0]),
    ("lag_1", lambda p: (p["lag"] or [None] * 3)[1]),
    ("lag_2", lambda p: (p["lag"] or [None] * 3)[2]),
    ("lag_total", lambda p: p["lag_total"]),
    ("desired", lambda p: p["desired"]),
    ("ready", lambda p: p["ready"]),
    ("members", lambda p: p["members"]),
    ("e2e_p95_estimate_s", lambda p: p["e2e_p95"]),
    ("db_p95_estimate_s", lambda p: p["db_p95"]),
    ("facts", lambda p: p["facts"]),
    ("dlq_copies", lambda p: p["dlq_copies"]),
    ("restarts", lambda p: p["restarts"]),
    ("mode", lambda p: p.get("mode")),
)


def workload_signature(config):
    keys = ("scenario", "seed", "schedule", "faults")
    return json.dumps({key: (config or {}).get(key) for key in keys}, sort_keys=True)


def eligibility(record):
    """Why a session can or cannot enter a formal comparison."""
    reasons = []
    if record.get("state") != "drained":
        reasons.append(f"session {record.get('state')}")
    if record.get("outcome") != "completed":
        reasons.append(f"sale {record.get('outcome') or 'unfinished'}")
    if record.get("reconciliation_status") != "passed":
        reasons.append(
            "not reconciled" if not record.get("reconciliation_status") else "reconciliation failed"
        )
    if record.get("segments"):
        reasons.append("scaling changed during the sale")
    if record.get("faults"):
        reasons.append("faults injected")
    if record.get("controls_changed"):
        reasons.append("pause or rate changed")
    return reasons


def summary_metrics(points):
    lags = [p["lag_total"] for p in points if p.get("lag_total") is not None]
    members = [p["members"] for p in points if p.get("members") is not None]
    ready = [p["ready"] for p in points if p.get("ready") is not None]
    return {
        "peak_lag": max(lags) if lags else None,
        "max_members": max(members) if members else None,
        "replica_seconds": sum(ready) if ready else None,
        "samples": len(points),
    }


class Component:
    def __init__(self, console):
        self.console = console
        self.comparison = None
        self.task = None
        register = console.coordinator.register
        register(
            "compare.start", self.start_comparison, "Run a formal comparison", self.check_compare
        )
        register("compare.cancel", self.cancel_comparison, "Cancel the comparison", mutating=False)

    def sales(self):
        return next(item for item in self.console.components if hasattr(item, "store"))

    def snapshot(self):
        return {"comparison": self.comparison}

    def tasks(self):
        return []

    # Routes -----------------------------------------------------------------------

    def routes(self, app):
        @app.get("/api/sessions/{identifier}")
        async def session(identifier: str):
            try:
                record = self.sales().store.load(identifier)
            except Rejected:
                return JSONResponse({"error": "Unknown session"}, status_code=404)
            folder = self.sales().store.path(identifier)
            frames = []
            path = folder / "telemetry.jsonl"
            if path.exists():
                for line in path.read_text(encoding="utf-8").splitlines():
                    try:
                        frames.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
            journal = []
            if (folder / "journal.jsonl").exists():
                journal = [
                    json.loads(line)
                    for line in (folder / "journal.jsonl").read_text().splitlines()
                    if line.strip()
                ]
            reconciliation = None
            if (folder / "reconciliation.json").exists():
                reconciliation = json.loads((folder / "reconciliation.json").read_text())
            return {
                "record": record,
                "frames": frames,
                "journal": journal,
                "reconciliation": reconciliation,
                "eligibility": eligibility(record),
                "metrics": summary_metrics([frame["point"] for frame in frames]),
            }

        @app.get("/api/sessions/{identifier}/export/{name}")
        async def export(identifier: str, name: str):
            if name not in EXPORTS:
                return JSONResponse({"error": "Unknown export"}, status_code=404)
            try:
                record = self.sales().store.load(identifier)
            except Rejected:
                return JSONResponse({"error": "Unknown session"}, status_code=404)
            folder = self.sales().store.path(identifier)
            disposition = {"Content-Disposition": f'attachment; filename="{identifier}-{name}"'}
            if name == "points.csv":
                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow([column for column, _ in POINT_COLUMNS])
                path = folder / "telemetry.jsonl"
                for line in path.read_text().splitlines() if path.exists() else []:
                    point = json.loads(line)["point"]
                    writer.writerow([read(point) for _, read in POINT_COLUMNS])
                return Response(buffer.getvalue(), media_type="text/csv", headers=disposition)
            if name == "configuration.json":
                return Response(
                    json.dumps(record, indent=2), media_type="application/json", headers=disposition
                )
            path = folder / name
            if not path.exists():
                return JSONResponse({"error": "Not recorded for this session"}, status_code=404)
            return FileResponse(path, headers=disposition)

        @app.get("/api/matrix")
        async def matrix():
            tables = {}
            for name in ("aggregate.csv", "comparison.csv"):
                path = RESULTS / name
                if path.exists():
                    tables[name.removesuffix(".csv")] = list(csv.DictReader(path.open()))
            provenance = RESULTS / "provenance.json"
            return {
                "tables": tables,
                "figures": sorted(name for name in FIGURES if (RESULTS / name).exists()),
                "provenance": json.loads(provenance.read_text()) if provenance.exists() else None,
                "read_only": True,
            }

        @app.get("/api/matrix/figure/{name}")
        async def figure(name: str):
            if name not in FIGURES or not (RESULTS / name).exists():
                return JSONResponse({"error": "Unknown figure"}, status_code=404)
            return FileResponse(RESULTS / name)

        @app.get("/api/comparisons")
        async def comparisons():
            rows = []
            if COMPARISONS.exists():
                for path in sorted(COMPARISONS.glob("*.json"), reverse=True)[:20]:
                    rows.append(json.loads(path.read_text()))
            sessions = self.sales().store.list(200)
            groups = {}
            for row in sessions:
                if not row.get("config"):
                    continue
                full = self.sales().store.load(row["id"])
                entry = {
                    "id": row["id"],
                    "created_at": row["created_at"],
                    "mode": (full.get("config") or {}).get("mode"),
                    "outcome": full.get("outcome"),
                    "eligible": not eligibility(full),
                    "reasons": eligibility(full),
                    "reconciliation": full.get("reconciliation"),
                    "comparison": full.get("comparison"),
                }
                groups.setdefault(workload_signature(full.get("config")), []).append(entry)
            return {
                "comparisons": rows,
                "groups": [
                    {"workload": json.loads(signature), "sessions": entries}
                    for signature, entries in groups.items()
                ],
            }

    # Formal comparison ------------------------------------------------------------

    def check_compare(self, params):
        if self.task is not None and not self.task.done():
            raise Rejected("A comparison is already running")
        modes = params.get("modes") or ["fixed", "cpu-hpa", "lag-keda"]
        allowed = {"fixed", "manual", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive"}
        if not isinstance(modes, list) or not modes or any(mode not in allowed for mode in modes):
            raise Rejected(
                "Choose modes from fixed, cpu-hpa, lag-keda, ml-reactive and ml-predictive"
            )

    async def start_comparison(self, operation):
        identifier = (
            "comparison-" + datetime.now(UTC).strftime("%Y%m%d%H%M%S") + "-" + os.urandom(3).hex()
        )
        modes = operation.params.get("modes") or ["fixed", "cpu-hpa", "lag-keda"]
        preset = operation.params.get("preset", "flash-sale")
        self.comparison = {
            "id": identifier,
            "preset": preset,
            "modes": modes,
            "state": "running",
            "started_at": time.time(),
            "runs": [],
            "step": "starting",
        }
        self.publish()
        self.task = asyncio.get_running_loop().create_task(self.orchestrate(preset, modes))
        return {"comparison": identifier}

    async def cancel_comparison(self, operation):
        if self.task is None or self.task.done():
            raise Rejected("No comparison is running")
        self.task.cancel()
        return {"cancelled": self.comparison["id"]}

    async def operation(self, kind, params, timeout=300):
        operation, _ = self.console.coordinator.submit(kind, params, f"cmp-{os.urandom(8).hex()}")
        deadline = time.monotonic() + timeout
        while operation.state in {"queued", "running"}:
            if time.monotonic() > deadline:
                raise RuntimeError(f"{kind} did not finish")
            await asyncio.sleep(0.5)
        if operation.state != "succeeded":
            raise RuntimeError(f"{kind} {operation.state}: {operation.error}")
        return operation.result

    async def wait_idle(self, timeout=900):
        deadline = time.monotonic() + timeout
        sales = self.sales()
        while sales.record is not None or ((self.console.hub.latest or {}).get("sale") or {}).get(
            "state"
        ) in {"preparing", "publishing", "paused", "stopping"}:
            if time.monotonic() > deadline:
                raise RuntimeError("The floor did not become idle")
            await asyncio.sleep(1)

    async def orchestrate(self, preset, modes):
        comparison = self.comparison
        sales = self.sales()
        try:
            for mode in modes:
                comparison["step"] = f"{mode}: waiting for an idle floor"
                self.publish()
                await self.wait_idle()
                comparison["step"] = f"{mode}: switching scaling mode"
                self.publish()
                await self.operation("scaling.mode", {"mode": mode}, timeout=300)
                await asyncio.sleep(20)  # let the new controller settle before load
                comparison["step"] = f"{mode}: running the {preset} sale"
                self.publish()
                result = await self.operation("sale.start", {"preset": preset})
                session = result["session"]
                record = sales.store.load(session)
                record["comparison"] = comparison["id"]
                sales.store.save(record)
                if sales.record and sales.record["id"] == session:
                    sales.record["comparison"] = comparison["id"]
                await self.wait_idle()
                comparison["step"] = f"{mode}: reconciling"
                self.publish()
                deadline = time.monotonic() + 600
                while not sales.store.load(session).get("reconciliation_status"):
                    if time.monotonic() > deadline:
                        raise RuntimeError("Reconciliation did not finish")
                    await asyncio.sleep(2)
                record = sales.store.load(session)
                frames = (sales.store.path(session) / "telemetry.jsonl").read_text().splitlines()
                points = [json.loads(line)["point"] for line in frames if line.strip()]
                comparison["runs"].append(
                    {
                        "mode": mode,
                        "session": session,
                        "run_name": record["run_name"],
                        "eligible": not eligibility(record),
                        "reasons": eligibility(record),
                        "exact_p95_s": (
                            (record.get("reconciliation") or {}).get("latency_s") or {}
                        ).get("p95"),
                        "stored_facts": (record.get("reconciliation") or {}).get("stored_facts"),
                        **summary_metrics(points),
                    }
                )
                self.publish()
            # Restore the baseline before reporting completion, never after it.
            comparison["step"] = "restoring fixed mode"
            self.publish()
            await self.operation("scaling.mode", {"mode": "fixed"}, timeout=300)
            comparison["state"] = "completed"
            comparison["step"] = "done"
        except asyncio.CancelledError:
            comparison["state"] = "cancelled"
            raise
        except Exception as error:  # noqa: BLE001 - shown to the user
            comparison["state"] = "failed"
            comparison["error"] = str(error)[:300]
        finally:
            comparison["ended_at"] = time.time()
            COMPARISONS.mkdir(parents=True, exist_ok=True)
            (COMPARISONS / f"{comparison['id']}.json").write_text(json.dumps(comparison, indent=2))
            self.publish()

    def publish(self):
        self.console.publish({"type": "component", "name": "comparison", "value": self.comparison})

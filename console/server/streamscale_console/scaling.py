"""Scaling operations: mode switches, manual replicas and override, settings.

Each operation asks the agent to start a transition and then waits until the agent
reports it verified (single-controller ownership) or failed, so the browser never
sees success before the cluster reflects it. Mode changes during a sale are
recorded on its session, which keeps that session out of formal comparisons.
"""

import asyncio
import time

from streamscale_console.operations import Rejected
from streamscale_console.telemetry import AgentRejected

MODES = {"fixed", "manual", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive"}


class Component:
    def __init__(self, console):
        self.console = console
        register = console.coordinator.register
        register(
            "scaling.mode", self.handler("scaling.mode"), "Change scaling mode", self.check_mode
        )
        register("scaling.replicas", self.handler("scaling.replicas"), "Set consumer replicas")
        register("scaling.override", self.handler("scaling.override"), "Enter manual override")
        register("scaling.release", self.handler("scaling.release"), "Release manual override")
        register("scaling.settings", self.handler("scaling.settings"), "Change scaling settings")
        register("scaling.reapply", self.handler("scaling.reapply"), "Reapply scaling mode")

    def snapshot(self):
        return {}

    def tasks(self):
        return []

    def routes(self, app):
        pass

    def check_mode(self, params):
        if params.get("mode") not in MODES:
            raise Rejected("Mode must be one of " + ", ".join(sorted(MODES)))

    def state(self):
        return (self.console.hub.latest or {}).get("scaling") or {}

    def handler(self, name):
        async def run(operation):
            client = self.console.hub.client
            if self.state().get("transition", {}).get("state") == "running":
                raise Rejected("Another scaling transition is still running")
            try:
                response = await client.call(
                    "POST", f"/v1/command/{name}", operation.params, timeout=20
                )
            except AgentRejected as error:
                raise Rejected(str(error)) from None
            label = response["result"]["transition"]
            self.console.coordinator.report(operation, label)
            deadline = time.monotonic() + 180
            last_step = None
            while time.monotonic() < deadline:
                await asyncio.sleep(0.5)
                transition = self.state().get("transition") or {}
                if transition.get("label") != label:
                    continue
                if transition.get("step") != last_step:
                    last_step = transition.get("step")
                    self.console.coordinator.report(operation, str(last_step))
                if transition.get("state") == "succeeded":
                    self.record_segment(operation)
                    return {"transition": label, "ownership": self.state().get("ownership")}
                if transition.get("state") in {"failed", "interrupted"}:
                    raise RuntimeError(transition.get("error") or "Scaling transition failed")
            raise RuntimeError("The scaling transition did not finish within three minutes")

        return run

    def record_segment(self, operation):
        sales = next((item for item in self.console.components if hasattr(item, "store")), None)
        if sales is None or sales.record is None:
            return
        sales.record.setdefault("segments", []).append(
            {"at": time.time(), "kind": operation.kind, "params": operation.params}
        )
        sales.store.save(sales.record)
        sales.publish()

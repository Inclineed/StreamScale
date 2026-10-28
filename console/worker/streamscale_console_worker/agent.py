"""In-cluster console agent: observation now; sale, scaling and fault commands later.

Reachable only through the host console's scoped port forward. Every /v1 request
needs the shared console token from the streamscale-console-token Secret. The
agent owns long-running work (publishing, fault recovery timers) so that closing
the browser or host server never leaves the pipeline in an unrecovered state.
"""

import hmac
import json
import os
import signal
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread
from urllib.parse import parse_qs, urlparse

from streamscale.config import Settings
from streamscale.run_artifacts import utc_now
from streamscale_console_worker import control
from streamscale_console_worker.kube import Kube, KubeError, ScopeError
from streamscale_console_worker.observe import Observer

VERSION = 1


class CommandError(ValueError):
    """A rejected request; the message is safe to show in the browser."""

    def __init__(self, message, status=409):
        super().__init__(message)
        self.status = status


class Agent:
    def __init__(self, settings, kube, root=control.ROOT):
        self.settings = settings
        self.kube = kube
        self.root = root
        self.started_at = utc_now()
        self.commands = {}
        self.queries = {}
        self.busy = []
        self.sections = {}
        self.runners = []
        self.components = {}
        self.observer = Observer(
            settings, kube, root, extras={"busy": self.busy, "sections": self.sections}
        )
        self.lock = threading.Lock()

    def register(self, component):
        """Attach a component exposing commands, queries, busy probes and sections."""
        self.commands.update(getattr(component, "commands", {}))
        self.queries.update(getattr(component, "queries", {}))
        self.busy.extend(getattr(component, "busy", []))
        self.sections.update(getattr(component, "sections", {}))
        self.runners.extend(getattr(component, "runners", []))

    def describe(self):
        return {
            "version": VERSION,
            "started_at": self.started_at,
            "pod": os.environ.get("POD_NAME"),
            "image": os.environ.get("CONSOLE_IMAGE"),
            "commands": sorted(self.commands),
            "queries": sorted(self.queries),
        }

    def telemetry(self, after=None):
        self.observer.touch()
        if after is None:
            latest = self.observer.latest() or self.observer.sample()
            return {"agent": self.describe(), "samples": [latest]}
        return {"agent": self.describe(), "samples": self.observer.since(after)}

    def command(self, name, body):
        if name not in self.commands:
            raise CommandError(f"Unknown command {name}", 404)
        # Commands are serialized; long work continues in the owning component.
        with self.lock:
            return self.commands[name](body)

    def query(self, name, parameters):
        if name not in self.queries:
            raise CommandError(f"Unknown query {name}", 404)
        return self.queries[name](parameters)


def handler_factory(agent, token):
    class Handler(BaseHTTPRequestHandler):
        def authorized(self):
            supplied = self.headers.get("Authorization", "")
            return hmac.compare_digest(supplied.encode(), ("Bearer " + token).encode())

        def reply(self, status, value):
            payload = json.dumps(value, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/health":
                return self.reply(200, {"status": "ok"})
            if not self.authorized():
                return self.reply(401, {"error": "Console token required"})
            parameters = {key: values[-1] for key, values in parse_qs(url.query).items()}
            try:
                if url.path == "/v1/telemetry":
                    after = parameters.get("after")
                    return self.reply(200, agent.telemetry(None if after is None else int(after)))
                if url.path.startswith("/v1/query/"):
                    return self.reply(
                        200, agent.query(url.path.removeprefix("/v1/query/"), parameters)
                    )
            except Exception as error:  # noqa: BLE001
                return self.failure(error)
            return self.reply(404, {"error": "Not found"})

        def do_POST(self):
            url = urlparse(self.path)
            if not self.authorized():
                return self.reply(401, {"error": "Console token required"})
            length = int(self.headers.get("Content-Length") or 0)
            if length > 64 * 1024:
                return self.reply(413, {"error": "Request too large"})
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
                if not isinstance(body, dict):
                    raise CommandError("Command bodies must be JSON objects", 400)
                if url.path.startswith("/v1/command/"):
                    result = agent.command(url.path.removeprefix("/v1/command/"), body)
                    return self.reply(200, {"status": "accepted", "result": result})
            except Exception as error:  # noqa: BLE001
                return self.failure(error)
            return self.reply(404, {"error": "Not found"})

        def failure(self, error):
            if isinstance(error, CommandError):
                return self.reply(error.status, {"error": str(error)})
            if isinstance(error, ScopeError):
                return self.reply(403, {"error": str(error)})
            if isinstance(error, KubeError):
                return self.reply(502, {"error": str(error)})
            if isinstance(error, (ValueError, json.JSONDecodeError)):
                return self.reply(400, {"error": str(error)[:300]})
            traceback.print_exc()
            return self.reply(500, {"error": type(error).__name__})

        def log_message(self, *args):
            pass

    return Handler


def build(settings, kube=None, root=control.ROOT):
    agent = Agent(settings, kube or Kube(), root)
    for name in ("sale", "scaling", "autoscale", "faults", "inspection", "reconcile"):
        # Later milestones add components; each is optional while it is absent.
        try:
            module = __import__(f"streamscale_console_worker.{name}", fromlist=["component"])
        except ModuleNotFoundError as error:
            if error.name != f"streamscale_console_worker.{name}":
                raise
            continue
        component = module.component(agent)
        agent.components[name] = component
        agent.register(component)
    return agent


def main():
    token = os.environ.get("CONSOLE_AGENT_TOKEN", "")
    if len(token) < 32:
        raise SystemExit("CONSOLE_AGENT_TOKEN must be provided by the console Secret")
    settings = Settings.from_env()
    root = control.ROOT
    (root / "runs").mkdir(parents=True, exist_ok=True)
    agent = build(settings, root=root)
    stop = Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    Thread(target=agent.observer.run_forever, args=(stop,), daemon=True).start()
    for runner in agent.runners:
        Thread(target=runner, args=(stop,), daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", 9200), handler_factory(agent, token))
    Thread(target=server.serve_forever, daemon=True).start()
    print(json.dumps({"agent": "ready", "at": utc_now()}), flush=True)
    stop.wait()
    server.shutdown()
    agent.observer.sql.close()


if __name__ == "__main__":
    main()

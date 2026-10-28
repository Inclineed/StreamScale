"""Loopback port forwards owned by this command and closed when it exits."""

import argparse
import signal
import socket
import subprocess
import time
import urllib.request
from contextlib import ExitStack, contextmanager
from threading import Event

from manage import NAMESPACE, OUTPUT, ROOT, TOOLS


def unused_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@contextmanager
def forward(role, port=None, resource=None):
    port = port or unused_port()
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            raise RuntimeError(f"Loopback port {port} is already in use") from None
    remote = {
        "prometheus": 9090,
        "grafana": 3000,
        "consumer": 9100,
        "exporter": 9100,
        "autoscaler": 9100,
    }[role]
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with (OUTPUT / f"forward-{role}-{port}.log").open("w") as log:
        process = subprocess.Popen(
            [
                str(TOOLS / "kubectl"),
                "--kubeconfig",
                str(ROOT / "artifacts/kubeconfig"),
                "--context",
                "kind-streamscale",
                "--namespace",
                NAMESPACE,
                "port-forward",
                resource or f"service/streamscale-{role}",
                f"{port}:{remote}",
                "--address",
                "127.0.0.1",
            ],
            stdout=log,
            stderr=log,
        )
        try:
            deadline = time.monotonic() + 20
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Port forward for {role} failed; see {log.name}")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            f"Port forward for {role} did not become ready"
                        ) from None
                    time.sleep(0.1)
            yield f"http://127.0.0.1:{port}"
        finally:
            process.terminate()
            try:
                process.wait(5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def fetch(url, headers=None):
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=15) as response:
        return response.read()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--grafana-port", type=int, default=13000)
    parser.add_argument("--prometheus-port", type=int, default=19090)
    args = parser.parse_args()
    stop = Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    with ExitStack() as stack:
        grafana = stack.enter_context(forward("grafana", args.grafana_port))
        prometheus = stack.enter_context(forward("prometheus", args.prometheus_port))
        print(f"Grafana: {grafana}/d/streamscale\nPrometheus: {prometheus}", flush=True)
        print(
            "Login: artifacts/k8s/credentials.json. "
            "Keep this command running; Ctrl+C stops forwards.",
            flush=True,
        )
        while not stop.wait(1):
            pass


if __name__ == "__main__":
    main()

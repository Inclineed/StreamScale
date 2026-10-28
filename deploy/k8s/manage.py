"""Project-scoped Docker/kind/Helm commands; never use the user's default kubeconfig."""

import argparse
import base64
import fcntl
import hashlib
import json
import os
import secrets
import subprocess
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from streamscale.run_artifacts import revision_snapshot, utc_now, write_json

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "artifacts/k8s"
CLUSTER = "streamscale"
NAMESPACE = "streamscale"
SECRET = "streamscale-local-credentials"
TOOLS = ROOT / ".tools/bin"


@contextmanager
def workflow():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with (OUTPUT / "workflow.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                "Another StreamScale cluster workflow is active; wait for it to finish"
            ) from None
        lock.seek(0)
        lock.truncate()
        json.dump({"pid": os.getpid(), "started_at": utc_now()}, lock)
        lock.flush()
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def previous_drained():
    info_file = OUTPUT / "deployment.json"
    if not info_file.exists():
        return
    if kubectl("get", "deployment", "streamscale-consumer", capture=True, check=False).returncode:
        return
    previous = json.loads(info_file.read_text())
    deadline = time.monotonic() + 60
    while True:
        result = kubectl(
            "exec",
            "deployment/streamscale-consumer",
            "--",
            "/app/.venv/bin/streamscale",
            "inspect",
            "--run-dir",
            "/runs/" + previous["run_name"],
            capture=True,
            check=False,
        )
        if result.returncode == 0:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Previous execution has not reconciled; keep its consumer running "
                "and recover it before publishing another run"
            )
        time.sleep(2)


def call(arguments, *, capture=False, input=None, check=True):
    return subprocess.run(
        [str(arg) for arg in arguments],
        cwd=ROOT,
        env=os.environ
        | {
            "KUBECONFIG": str(ROOT / "artifacts/kubeconfig"),
            "HELM_CACHE_HOME": str(ROOT / ".tools/helm/cache"),
            "HELM_CONFIG_HOME": str(ROOT / ".tools/helm/config"),
            "HELM_DATA_HOME": str(ROOT / ".tools/helm/data"),
        },
        input=input,
        text=True,
        capture_output=capture,
        check=check,
    )


def kubectl(*arguments, **kwargs):
    return call(
        [TOOLS / "kubectl", "--context", f"kind-{CLUSTER}", "-n", NAMESPACE, *arguments], **kwargs
    )


def build():
    metadata = revision_snapshot()
    tag = hashlib.sha256(
        (
            metadata["source_sha256"]
            + metadata["git_commit"]
            + str(metadata["git_dirty"])
            + (ROOT / "Dockerfile").read_text()
        ).encode()
    ).hexdigest()[:16]
    image = f"streamscale:{tag}"
    call(
        [
            "docker",
            "build",
            "--tag",
            image,
            "--build-arg",
            f"GIT_COMMIT={metadata['git_commit']}",
            "--build-arg",
            f"GIT_DIRTY={str(metadata['git_dirty']).lower()}",
            "--build-arg",
            f"SOURCE_SHA256={metadata['source_sha256']}",
            "--build-arg",
            f"IMAGE_REF={image}",
            ".",
        ]
    )
    metadata["image"] = image
    metadata["image_id"] = call(
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"], capture=True
    ).stdout.strip()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    write_json(OUTPUT / "image.json", metadata)
    return metadata


def cluster():
    names = call([TOOLS / "kind", "get", "clusters"], capture=True).stdout.splitlines()
    if CLUSTER not in names:
        call(
            [
                TOOLS / "kind",
                "create",
                "cluster",
                "--name",
                CLUSTER,
                "--config",
                "deploy/k8s/kind.yaml",
                "--kubeconfig",
                "artifacts/kubeconfig",
                "--wait",
                "180s",
            ]
        )
    else:
        call(
            [
                TOOLS / "kind",
                "export",
                "kubeconfig",
                "--name",
                CLUSTER,
                "--kubeconfig",
                "artifacts/kubeconfig",
            ]
        )


def credentials():
    existing = kubectl("get", "secret", SECRET, "-o", "json", capture=True, check=False)
    if existing.returncode == 0:
        credential = json.loads(existing.stdout)
        values = {
            key: base64.b64decode(value).decode() for key, value in credential["data"].items()
        }
    else:
        password = secrets.token_urlsafe(24)
        values = {
            "POSTGRES_PASSWORD": password,
            "DATABASE_URL": f"postgresql://streamscale:{password}@streamscale-postgres:5432/streamscale",
            "GF_SECURITY_ADMIN_PASSWORD": secrets.token_urlsafe(24),
        }
        kubectl(
            "create",
            "-f",
            "-",
            input=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "type": "Opaque",
                    "metadata": {"name": SECRET, "namespace": NAMESPACE},
                    "stringData": values,
                }
            ),
        )
    path = OUTPUT / "credentials.json"
    # Private file, never print passwords or embed them in Helm values/history.
    with open(
        path, "w", encoding="utf-8", opener=lambda name, flags: os.open(name, flags, 0o600)
    ) as out:
        json.dump(
            {"grafana_user": "admin", "grafana_password": values["GF_SECURITY_ADMIN_PASSWORD"]},
            out,
            indent=2,
        )
    path.chmod(0o600)
    print(f"Grafana login saved in {path.relative_to(ROOT)}")


def clear_scaling():
    # Finish/drain the previous run before switching. Stop the ML controller first so it
    # cannot rescale the consumer after the reset below; Helm recreates it for ML modes.
    kubectl("delete", "deployment", "streamscale-autoscaler", "--ignore-not-found", "--wait=true")
    if kubectl("get", "crd", "scaledobjects.keda.sh", capture=True, check=False).returncode == 0:
        kubectl(
            "delete", "scaledobject", "streamscale-consumer", "--ignore-not-found", "--wait=true"
        )
    kubectl(
        "delete",
        "hpa",
        "streamscale-cpu",
        "keda-hpa-streamscale-consumer",
        "--ignore-not-found",
        "--wait=true",
    )
    if (
        kubectl("get", "deployment", "streamscale-consumer", capture=True, check=False).returncode
        == 0
    ):
        kubectl("scale", "deployment/streamscale-consumer", "--replicas=1")


def deploy(metadata, args, wait=True, run_name=None):
    if run_name is None:
        previous_drained()
    if kubectl("get", "namespace", NAMESPACE, capture=True, check=False).returncode:
        kubectl("create", "namespace", NAMESPACE)
    credentials()
    clear_scaling()
    name = run_name or "run-" + datetime.now(UTC).strftime("%Y%m%d%H%M%S") + "-" + uuid4().hex[:8]
    info = metadata | {
        "run_name": name,
        "namespace": NAMESPACE,
        "context": f"kind-{CLUSTER}",
        "mode": args.mode,
        "scenario": args.scenario,
        "seed": args.seed,
    }
    write_json(OUTPUT / "deployment.json", info)
    arguments = [
        TOOLS / "helm",
        "upgrade",
        "--install",
        "streamscale",
        "deploy/k8s/chart",
        "--namespace",
        NAMESPACE,
        "--kube-context",
        f"kind-{CLUSTER}",
        "--server-side=false",
        "--set-string",
        f"image.tag={metadata['image'].split(':')[1]}",
        "--set-string",
        f"run.name={name}",
        "--set",
        f"run.orders={args.orders}",
        "--set-string",
        f"run.profile={args.profile}",
        "--set-string",
        f"scaling.mode={args.mode}",
        "--set-string",
        f"run.scenario={args.scenario}",
        "--set",
        f"run.seed={args.seed}",
        "--set",
        f"run.paced={str(args.paced).lower()}",
        "--set",
        f"run.startDelay={args.start_delay}",
        "--timeout",
        "10m",
    ]
    for key, value in sorted((getattr(args, "autoscaler", None) or {}).items()):
        arguments.extend(["--set-json", f"autoscaler.{key}={json.dumps(value)}"])
    arguments.extend(
        [
            "--set-json",
            "run.schedule=" + json.dumps(getattr(args, "schedule", [])),
            "--set-json",
            "run.configuration=" + json.dumps(getattr(args, "configuration", {})),
        ]
    )
    if wait:
        arguments.extend(["--wait", "--wait-for-jobs"])
    call(arguments)
    print(f"Cluster run ready: /runs/{name}; use make k8s-verify and make k8s-ui")
    return info


def up(args):
    metadata = build()
    cluster()
    call([TOOLS / "kind", "load", "docker-image", metadata["image"], "--name", CLUSTER])
    if args.mode in {"cpu-hpa", "lag-keda"}:
        from addons import main as install_addons

        install_addons()
    return deploy(metadata, args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("build", "up", "status", "logs", "credentials"))
    parser.add_argument("--orders", type=int, default=200)
    parser.add_argument(
        "--mode",
        choices=("fixed", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive"),
        default="fixed",
    )
    parser.add_argument(
        "--scenario",
        choices=("steady", "flash-sale", "overload-recovery", "ramp", "repeated-sales"),
        default="steady",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--paced", action="store_true")
    parser.add_argument("--start-delay", type=int, default=0)
    parser.add_argument(
        "--profile", choices=("normal", "correctness", "fixtures"), default="correctness"
    )
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    if args.command == "build":
        build()
    elif args.command == "up":
        with workflow():
            up(args)
    elif args.command == "credentials":
        credentials()
    elif args.command == "logs":
        kubectl("logs", "deployment/streamscale-consumer", "--tail=80")
    else:
        kubectl("get", "pods,jobs,svc,pvc")


if __name__ == "__main__":
    main()

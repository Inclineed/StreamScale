"""Exercise each alternative controller and retain observed replica-bound evidence."""

import json
import time
from types import SimpleNamespace

from access import fetch, forward
from addons import main as install_addons
from manage import CLUSTER, OUTPUT, TOOLS, build, call, cluster, deploy, kubectl, workflow

from streamscale.run_artifacts import utc_now, write_json

MODES = ("fixed", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive")
# M7's frozen v2 matrix keeps exactly these three modes.
V2_MODES = ("fixed", "cpu-hpa", "lag-keda")
ML_MODES = ("ml-reactive", "ml-predictive")


def autoscaler_state(objects, mode):
    """Check the ML controller Deployment: present and matching only in ML modes."""
    controllers = [
        row
        for row in objects
        if row["kind"] == "Deployment" and row["metadata"]["name"] == "streamscale-autoscaler"
    ]
    if mode not in ML_MODES:
        if controllers:
            raise RuntimeError(f"{mode} mode has an active ML autoscaler")
        return None
    if len(controllers) != 1:
        raise RuntimeError(f"{mode} mode needs exactly one ML autoscaler Deployment")
    controller = controllers[0]
    template = controller["spec"]["template"]
    policy = mode.removeprefix("ml-")
    container = template["spec"]["containers"][0]
    arguments = container.get("args", [])
    if template["metadata"]["annotations"].get("streamscale/policy") != policy or (
        arguments[arguments.index("--policy") + 1] != policy
    ):
        raise RuntimeError(f"{mode} autoscaler runs the wrong policy")
    mounts = container.get("volumeMounts", [])
    volumes = template["spec"].get("volumes", [])
    if mounts or volumes or container.get("env"):
        raise RuntimeError("ML autoscaler must not mount run artifacts or credentials")
    pods = [
        row
        for row in objects
        if row["kind"] == "Pod"
        and row["metadata"].get("labels", {}).get("app") == "streamscale-autoscaler"
        and not row["metadata"].get("deletionTimestamp")
    ]
    return {
        "policy": policy,
        "pods": len(pods),
        "ready": int(controller.get("status", {}).get("readyReplicas", 0) or 0),
        "image": container["image"],
    }


def mode_snapshot(mode):
    objects = json.loads(
        kubectl("get", "deployment,pod,hpa,scaledobject", "-o", "json", capture=True).stdout
    )["items"]
    autoscaler = autoscaler_state(objects, mode)
    deployment = next(
        row
        for row in objects
        if row["kind"] == "Deployment" and row["metadata"]["name"] == "streamscale-consumer"
    )
    hpas = [
        row
        for row in objects
        if row["kind"] == "HorizontalPodAutoscaler"
        and row["spec"]["scaleTargetRef"]["name"] == "streamscale-consumer"
    ]
    scaled = [
        row
        for row in objects
        if row["kind"] == "ScaledObject"
        and row["spec"]["scaleTargetRef"]["name"] == "streamscale-consumer"
    ]
    desired = deployment["spec"]["replicas"]
    run_name = deployment["spec"]["template"]["metadata"]["annotations"]["streamscale/run-name"]
    pods = [
        row
        for row in objects
        if row["kind"] == "Pod"
        and row["metadata"].get("labels", {}).get("app") == "streamscale-consumer"
        and row["metadata"].get("annotations", {}).get("streamscale/run-name") == run_name
        and not row["metadata"].get("deletionTimestamp")
    ]
    ready = sum(
        any(
            condition["type"] == "Ready" and condition["status"] == "True"
            for condition in pod["status"].get("conditions", [])
        )
        for pod in pods
    )
    if not 1 <= desired <= (1 if mode == "fixed" else 3) or ready > 3:
        raise RuntimeError("Consumer replica target escaped its mode bounds")
    if mode == "fixed":
        if hpas or scaled:
            raise RuntimeError("Fixed mode has an active autoscaler")
    elif mode == "cpu-hpa":
        if len(hpas) != 1 or scaled or hpas[0]["metadata"]["name"] != "streamscale-cpu":
            raise RuntimeError("CPU-HPA mode is not isolated")
        if hpas[0]["spec"]["metrics"][0]["type"] != "Resource":
            raise RuntimeError("CPU-HPA is not driven by resource metrics")
    elif mode in ML_MODES:
        if hpas or scaled:
            raise RuntimeError(f"{mode} mode shares the consumer with another autoscaler")
    elif mode == "lag-keda":
        if len(scaled) != 1 or any(
            row["metadata"]["name"] != "keda-hpa-streamscale-consumer" for row in hpas
        ):
            raise RuntimeError("Lag-KEDA mode is not isolated")
        spec = scaled[0]["spec"]
        if spec["minReplicaCount"] != 1 or spec["maxReplicaCount"] != 3:
            raise RuntimeError("KEDA replica bounds differ from 1–3")
    else:
        raise RuntimeError(f"Unknown scaling mode {mode!r}")
    for hpa in hpas:
        if hpa["spec"]["minReplicas"] != 1 or hpa["spec"]["maxReplicas"] != 3:
            raise RuntimeError("HPA replica bounds differ from 1–3")
    return {
        "at": utc_now(),
        "mode": mode,
        "desired": desired,
        "ready": ready,
        "hpas": hpas,
        "scaledobjects": scaled,
        "autoscaler": autoscaler,
        "pods": pods,
    }


def serving_consumer():
    """A Ready, non-terminating consumer pod, so exec never lands on a pod scaling in."""
    pods = json.loads(
        kubectl("get", "pods", "-l", "app=streamscale-consumer", "-o", "json", capture=True).stdout
    )["items"]
    for pod in pods:
        ready = any(
            condition["type"] == "Ready" and condition["status"] == "True"
            for condition in pod["status"].get("conditions", [])
        )
        if ready and not pod["metadata"].get("deletionTimestamp"):
            return "pod/" + pod["metadata"]["name"]
    return None


def inspect(directory, attempts=10, wait=3.0):
    """Reconcile one run. Retries briefly: a scale-in can remove the exec target mid-call.

    Returns None only if every attempt fails, so a real mismatch is still reported.
    """
    for attempt in range(attempts):
        target = serving_consumer()
        if target is not None:
            result = kubectl(
                "exec",
                target,
                "--",
                "/app/.venv/bin/streamscale",
                "inspect",
                "--run-dir",
                directory,
                capture=True,
                check=False,
            )
            if result.returncode == 0:
                return json.loads(result.stdout)
        if attempt + 1 < attempts:
            time.sleep(wait)
    return None


def decisions(folder):
    """Retain the ML controller's recent decisions for an ML-mode exercise."""
    with forward("autoscaler") as endpoint:
        payload = json.loads(fetch(endpoint + "/decisions"))
    write_json(folder / "autoscaler-decisions.json", payload)
    return payload


def exercise(metadata, mode, orders=3000, timeout=300):
    args = SimpleNamespace(
        mode=mode,
        scenario="steady",
        seed=42,
        orders=orders,
        profile="correctness",
        paced=False,
        start_delay=3,
    )
    info = deploy(metadata, args, wait=False)
    folder = OUTPUT / info["run_name"]
    folder.mkdir(exist_ok=True)
    deadline = time.monotonic() + timeout
    maximum = 0
    reconciliation = None
    with (folder / "scaling-samples.jsonl").open("w") as out:
        while time.monotonic() < deadline:
            sample = mode_snapshot(mode)
            maximum = max(maximum, sample["ready"])
            out.write(json.dumps(sample) + "\n")
            out.flush()
            job = json.loads(
                kubectl(
                    "get",
                    "job",
                    "streamscale-workload-" + info["run_name"],
                    "-o",
                    "json",
                    capture=True,
                ).stdout
            )
            if job["status"].get("failed"):
                raise RuntimeError("Scaling workload failed; inspect its Job logs")
            if job["status"].get("succeeded"):
                reconciliation = inspect("/runs/" + info["run_name"])
                if reconciliation and (mode == "fixed" or maximum > 1):
                    break
            time.sleep(2)
    if reconciliation is None:
        raise RuntimeError(f"{mode} did not reconcile before the scaling deadline")
    if mode != "fixed" and maximum < 2:
        raise RuntimeError(f"{mode} never added a ready consumer under backlog")
    if mode in ML_MODES:
        recorded = decisions(folder)["decisions"]
        if not any(item["applied"] and item["replicas"] > 1 for item in recorded):
            raise RuntimeError(f"{mode} controller recorded no applied scale-up")
    write_json(folder / "inspection.json", reconciliation)
    result = {
        "status": "passed",
        "mode": mode,
        "run_name": info["run_name"],
        "run_id": reconciliation["run_id"],
        "max_ready_replicas": maximum,
        "stored_facts": reconciliation["stored_facts"],
        "replica_bounds": [1, 1 if mode == "fixed" else 3],
        "mode_isolation": True,
        "verified_at": utc_now(),
    }
    write_json(folder / "scaling-verification.json", result)
    print(json.dumps(result), flush=True)
    return result


def run():
    metadata = build()
    cluster()
    call([TOOLS / "kind", "load", "docker-image", metadata["image"], "--name", CLUSTER])
    install_addons()
    results = [exercise(metadata, mode) for mode in MODES]
    write_json(OUTPUT / "scaling-verification.json", {"status": "passed", "modes": results})
    # Leave the standard dashboard demo and one fixed consumer available.
    args = SimpleNamespace(
        mode="fixed",
        scenario="steady",
        seed=42,
        orders=200,
        profile="correctness",
        paced=False,
        start_delay=0,
    )
    deploy(metadata, args)
    from verify import main as verify

    verify()


def main():
    with workflow():
        run()


if __name__ == "__main__":
    main()

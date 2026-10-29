"""Pilot, frozen 27-cell matrix, exact reconciliation, and restart-safe evidence export."""

import argparse
import csv
import hashlib
import json
import random
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from prometheus_client.parser import text_string_to_metric_families

from streamscale.run_artifacts import (
    read_acknowledgements,
    revision_snapshot,
    utc_now,
    verify_manifest,
    write_json,
)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "deploy/k8s"))

from access import fetch, forward  # noqa: E402
from addons import main as install_addons  # noqa: E402
from manage import (  # noqa: E402
    CLUSTER,
    NAMESPACE,
    TOOLS,
    build,
    call,
    cluster,
    deploy,
    kubectl,
    workflow,
)  # noqa: E402
from scaling import (  # noqa: E402
    ML_MODES,
    V2_MODES,
    inspect,
    mode_snapshot,
    serving_consumer,
)
from streamscale_workload.shapes import (  # noqa: E402
    SHAPE_VERSION,
    require_namespace,
    sale_starts,
)
from streamscale_workload.shapes import schedule as shape_schedule  # noqa: E402

from experiments.metrics import correct, epoch, quantity, summarize  # noqa: E402

PROTOCOLS = {
    "v2": ROOT / "experiments/protocol.json",
    "v3": ROOT / "experiments/protocols/v3.json",
}
# v2 keeps its original location so M7 evidence and latest.path are never repointed.
DESTINATIONS = {
    "v2": ROOT / "artifacts/experiments",
    "v3": ROOT / "artifacts/experiments/v3",
}
DESTINATION = DESTINATIONS["v2"]
LATEST = DESTINATION / "latest.path"


def load_protocol(name):
    if name not in PROTOCOLS:
        raise ValueError(f"Unknown protocol {name!r}")
    return json.loads(PROTOCOLS[name].read_text())


def protocol_modes(protocol):
    return tuple(protocol.get("modes", V2_MODES))


def scratch_destination(name):
    return DESTINATIONS[name] / "scratch"


def fingerprint():
    digest = hashlib.sha256(revision_snapshot()["source_sha256"].encode())
    paths = [ROOT / "experiments" / name for name in ("run.py", "metrics.py", "protocol.json")]
    paths += sorted((ROOT / "experiments/protocols").glob("*.json"))
    paths += [
        path
        for path in (ROOT / "deploy/k8s").rglob("*")
        if path.suffix in {".py", ".json", ".yaml"}
    ]
    for path in sorted(paths):
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0" + path.read_bytes())
    return digest.hexdigest()


def plan(protocol):
    cells = [
        {
            "mode": mode,
            "scenario": scenario,
            "repetition": repeat,
            "key": f"{scenario}-{mode}-{repeat}",
        }
        for repeat in range(1, protocol["repetitions"] + 1)
        for scenario in protocol["scenarios"]
        for mode in protocol_modes(protocol)
    ]
    random.Random(protocol["order_seed"]).shuffle(cells)
    return cells


def select(cells, modes=None, scenarios=None, repetitions=None):
    """Subset of a plan for scratch campaigns; order is preserved."""
    return [
        cell
        for cell in cells
        if (not modes or cell["mode"] in modes)
        and (not scenarios or cell["scenario"] in scenarios)
        and (not repetitions or cell["repetition"] <= repetitions)
    ]


def shape_seed(protocol, scenario, repetition):
    """Evaluation-namespace shape seed, shared by every mode of one (scenario, repetition)."""
    seed = protocol["scenarios"][scenario]["seed"]
    return require_namespace(
        f"{protocol['shape_seed_namespace']}:{seed * 100 + repetition}",
        protocol["shape_seed_namespace"],
    )


def model_identity():
    from streamscale.autoscale.forecast import load as load_model

    model = load_model()
    trained = model.metadata.get("training_seeds", "")
    if not str(trained).startswith("train:"):
        raise RuntimeError("The packaged forecaster does not record training-only seeds")
    return {
        "sha256": model.sha256,
        "quantile": model.quantile,
        "training_seeds": trained,
        "holdout_seeds": model.metadata.get("holdout_seeds"),
        "tuning": model.metadata.get("tuning"),
    }


def freeze_v3(protocol, capacity, steady, surge):
    frozen = json.loads(json.dumps(protocol))
    frozen["pilot_capacity_per_second"] = capacity
    frozen["rates"] = {"steady": steady, "surge": surge}
    frozen["shape_version"] = SHAPE_VERSION
    for name, scenario in frozen["scenarios"].items():
        scenario["repetitions"] = {}
        for repeat in range(1, protocol["repetitions"] + 1):
            seed = shape_seed(protocol, name, repeat)
            phases = shape_schedule(name, seed, steady, surge)
            starts = sale_starts(phases, steady)
            elapsed, sale_end = 0, None
            for seconds, rate in phases:
                elapsed += seconds
                if rate > steady:
                    sale_end = elapsed
            scenario["repetitions"][str(repeat)] = {
                "shape_seed": seed,
                "event_seed": scenario["seed"] * 10 + repeat,
                "schedule": phases,
                "sale_starts": starts,
                "load_transition_seconds": starts[0] if starts else 0,
                "recovery_transition_seconds": sale_end
                if scenario.get("recovery_after_sale")
                else None,
            }
    frozen["autoscaler"] = dict(protocol["autoscaler"], capacity=capacity)
    frozen["model"] = model_identity()
    pinned = protocol.get("model_sha256")
    if pinned and frozen["model"]["sha256"] != pinned:
        raise RuntimeError(
            "The packaged forecaster differs from the model pinned in the protocol; "
            "retraining after the pilot needs a new protocol version"
        )
    frozen["frozen_at"] = utc_now()
    return frozen


def freeze(protocol, capacity):
    if capacity <= 0:
        raise ValueError("Pilot capacity must be positive")
    steady = max(2, int(capacity * protocol["steady_capacity_factor"]) // 2 * 2)
    surge = max(
        steady + 2,
        min(
            protocol["maximum_surge_rate"],
            int(capacity * protocol["surge_capacity_factor"]) // 2 * 2,
        ),
    )
    if protocol["protocol_version"] >= 3:
        return freeze_v3(protocol, capacity, steady, surge)
    frozen = json.loads(json.dumps(protocol))
    frozen["pilot_capacity_per_second"] = capacity
    frozen["rates"] = {"steady": steady, "surge": surge}
    for scenario in frozen["scenarios"].values():
        scenario["schedule"] = [
            [seconds, frozen["rates"][rate]] for seconds, rate in scenario["phases"]
        ]
    frozen["frozen_at"] = utc_now()
    return frozen


def raw_metrics(payload, run_id):
    values = {}
    for family in text_string_to_metric_families(payload):
        for sample in family.samples:
            if sample.labels.get("run_id") == run_id:
                values.setdefault(sample.name, []).append(sample)
    lag = {
        sample.labels["partition"]: sample.value
        for sample in values.get("streamscale_kafka_lag", [])
    }
    success = values.get("streamscale_exporter_last_success_unixtime", [])
    return {
        "lag": lag if set(lag) == {"0", "1", "2"} else None,
        "exporter_sample_at": success[0].value if success else None,
        "members": sum(sample.value for sample in values.get("streamscale_consumer_replicas", [])),
    }


def current_exporter(name, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pods = json.loads(
            kubectl(
                "get", "pods", "-l", "app=streamscale-exporter", "-o", "json", capture=True
            ).stdout
        )["items"]
        for pod in pods:
            if (
                pod["metadata"].get("annotations", {}).get("streamscale/run-name") == name
                and pod["status"].get("phase") == "Running"
                and all(
                    status.get("started") is True
                    for status in pod["status"].get("containerStatuses", [{}])
                )
                and not pod["metadata"].get("deletionTimestamp")
            ):
                return pod["metadata"]["name"]
        time.sleep(0.2)
    raise RuntimeError("The new execution's exporter did not start")


def pod_resources(pods):
    wanted = {pod["metadata"]["name"] for pod in pods}
    result = kubectl(
        "get",
        "--raw",
        f"/apis/metrics.k8s.io/v1beta1/namespaces/{NAMESPACE}/pods",
        capture=True,
        check=False,
    )
    if result.returncode:
        return None
    rows = [pod for pod in json.loads(result.stdout)["items"] if pod["metadata"]["name"] in wanted]
    if not rows:
        return None
    return {
        "cpu_cores": sum(
            quantity(container["usage"]["cpu"], "cpu")
            for pod in rows
            for container in pod["containers"]
        ),
        "memory_bytes": sum(
            quantity(container["usage"]["memory"], "memory")
            for pod in rows
            for container in pod["containers"]
        ),
        "resource_api_samples": rows,
    }


def copy_run(name, destination):
    for filename in ("run.json", "manifest.jsonl", "acknowledgements.jsonl", "inspection.json"):
        result = kubectl(
            "exec",
            serving_consumer() or "deployment/streamscale-consumer",
            "--",
            "/bin/cat",
            f"/runs/{name}/{filename}",
            capture=True,
        )
        (destination / filename).write_text(result.stdout)
    run = json.loads((destination / "run.json").read_text())
    verify_manifest(destination, run)
    read_acknowledgements(destination, run)
    return run


def scrape_history(run, samples, destination):
    from urllib.parse import urlencode

    from verify import query

    with forward("prometheus") as endpoint:
        expressions = [
            name + '{run_id="' + run["run_id"] + '"}'
            for name in (
                "streamscale_kafka_lag",
                "streamscale_consumer_replicas",
                "streamscale_accepted_total",
                "streamscale_duplicate_total",
                "streamscale_invalid_total",
                "streamscale_dlq_total",
                "streamscale_failed_total",
                "streamscale_end_to_end_seconds_bucket",
                "streamscale_db_seconds_bucket",
            )
        ]
        results = {}
        for expression in expressions:
            params = {
                "query": expression,
                "start": epoch(run["publish_started_at"]),
                "end": samples[-1]["time"],
                "step": 1,
            }
            results[expression] = json.loads(
                fetch(endpoint + "/api/v1/query_range?" + urlencode(params))
            )
        write_json(destination / "prometheus-range.json", results)
        failed = query(
            endpoint, 'sum(increase(streamscale_failed_total{run_id="' + run["run_id"] + '"}[30m]))'
        )
        write_json(destination / "handling-failures.json", {"samples": failed})


def deploy_arguments(cell, configuration):
    schedule = configuration["schedule"]
    count = sum(seconds * rate for seconds, rate in schedule)
    if count % 2:
        raise RuntimeError("The frozen schedule must contain an even number of logical events")
    return SimpleNamespace(
        mode=cell["mode"],
        scenario=cell["scenario"],
        seed=cell["seed"],
        orders=count // 2,
        profile=configuration["profile"],
        paced=configuration["paced"],
        start_delay=8,
        schedule=schedule,
        configuration=configuration,
        autoscaler=chart_autoscaler(configuration.get("autoscaler")),
    )


def chart_autoscaler(settings):
    """Frozen controller settings as Helm values (only rendered in ML modes)."""
    if not settings:
        return None
    return {
        "capacity": settings["capacity"],
        "targetDelaySeconds": settings["target_delay_seconds"],
        "readyDelaySeconds": settings["ready_delay_seconds"],
        "scaleDownHoldSeconds": settings["scale_down_hold_seconds"],
        "scaleDownMargin": settings["scale_down_margin"],
        "stableGroupSeconds": settings["stable_group_seconds"],
        "variant": settings.get("variant", "full"),
        "scaleDownIntervalSeconds": settings["scale_down_interval_seconds"],
    }


def autoscaler_decisions(destination):
    """Retain the ML controller's decision log; it lives only in the controller's memory."""
    with forward("autoscaler") as endpoint:
        payload = json.loads(fetch(endpoint + "/decisions"))
    write_json(destination / "autoscaler-decisions.json", payload)
    return payload


def pause_autoscaler(destination):
    kubectl("scale", "deployment/streamscale-autoscaler", "--replicas=0")
    kubectl(
        "wait",
        "--for=delete",
        "pod",
        "-l",
        "app=streamscale-autoscaler",
        "--timeout=60s",
        check=False,
    )
    write_json(destination / "autoscaler-paused.json", {"paused_at": utc_now()})


def capture(metadata, cell, configuration, campaign):
    schedule = configuration["schedule"]
    args = deploy_arguments(cell, configuration)
    info = deploy(metadata, args, wait=False)
    destination = campaign / info["run_name"]
    destination.mkdir()
    write_json(destination / "deployment.json", info)
    write_json(destination / "configuration.json", configuration)
    write_json(
        campaign / "active-run.json",
        {"cell": cell, "deployment": info, "directory": str(destination)},
    )
    print(
        f"Started {configuration['phase']} {cell['mode']} / {cell['scenario']}: {info['run_name']}",
        flush=True,
    )
    run = None
    zeros = 0
    last_fresh = None
    next_resource = 0
    samples = []
    began = time.monotonic()
    deadline = (
        began
        + 180
        + sum(seconds for seconds, _ in schedule)
        + configuration["drain_timeout_seconds"]
    )
    pod = current_exporter(info["run_name"])
    with (
        forward("exporter", resource="pod/" + pod) as endpoint,
        (destination / "samples.jsonl").open("w") as log,
        (destination / "exporter-raw.txt").open("w") as raw,
    ):
        while time.monotonic() < deadline:
            iteration = time.monotonic()
            snapshot = mode_snapshot(cell["mode"])
            sample = snapshot | {
                "time": time.time(),
                "lag": None,
                "exporter_sample_at": None,
                "cpu_cores": None,
                "memory_bytes": None,
            }
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
                raise RuntimeError("The experiment publisher Job failed")
            if run is None or run.get("status") != "published":
                result = kubectl(
                    "exec",
                    "deployment/streamscale-exporter",
                    "--",
                    "/bin/cat",
                    "/runs/" + info["run_name"] + "/run.json",
                    capture=True,
                    check=False,
                )
                if result.returncode == 0:
                    run = json.loads(result.stdout)
            if run and run["status"] in {"publishing", "published"}:
                try:
                    payload = fetch(endpoint + "/metrics").decode()
                    raw.write(f"# captured_at {sample['time']}\n" + payload)
                    sample.update(raw_metrics(payload, run["run_id"]))
                    if (
                        sample["exporter_sample_at"] is None
                        or sample["time"] - sample["exporter_sample_at"] > 3
                    ):
                        sample["lag"] = None
                except OSError as error:
                    sample["metric_error"] = type(error).__name__
            if iteration >= next_resource:
                next_resource = iteration + configuration["resource_interval_seconds"]
                resource = pod_resources(snapshot["pods"])
                if resource:
                    sample.update(resource)
            samples.append(sample)
            log.write(json.dumps(sample) + "\n")
            log.flush()
            if sample["lag"] is None:
                zeros = 0
            elif sample["exporter_sample_at"] != last_fresh:
                if last_fresh is not None and sample["exporter_sample_at"] - last_fresh > 2:
                    zeros = 0
                last_fresh = sample["exporter_sample_at"]
                zeros = zeros + 1 if sum(sample["lag"].values()) == 0 else 0
            if (
                run
                and run["status"] == "published"
                and job["status"].get("succeeded")
                and zeros >= 5
                and sample["time"]
                >= epoch(run["publish_started_at"]) + sum(seconds for seconds, _ in schedule)
            ):
                break
            if len(samples) % 15 == 0:
                lag_value = sum(sample["lag"].values()) if sample["lag"] else "waiting"
                print(
                    f"  {info['run_name']}: ready={sample['ready']} lag={lag_value}",
                    flush=True,
                )
            time.sleep(
                max(0, configuration["sample_interval_seconds"] - (time.monotonic() - iteration))
            )
        else:
            raise RuntimeError("The run did not drain before its experiment deadline")
    decisions = None
    if cell["mode"] in ML_MODES:
        if configuration["phase"] == "measured":
            decisions = autoscaler_decisions(destination)
        # The measured window has drained. Pause the controller so the reconciliation and
        # controlled-restart checks run on a fixed, stable group instead of racing a
        # scale-in. The next deploy's clear_scaling removes it and Helm recreates it.
        pause_autoscaler(destination)
    before = inspect("/runs/" + info["run_name"])
    if before is None:
        raise RuntimeError("Pre-restart SQL/DLQ reconciliation failed")
    write_json(destination / "before-restart-inspection.json", before)
    run = copy_run(info["run_name"], destination)
    result = kubectl(
        "exec",
        serving_consumer() or "deployment/streamscale-consumer",
        "--",
        "/app/.venv/bin/streamscale",
        "measurements",
        "--run-dir",
        "/runs/" + info["run_name"],
        capture=True,
    )
    measurements = json.loads(result.stdout)
    write_json(destination / "sql-measurements.json", measurements)
    scrape_history(run, samples, destination)
    # Restart is after the measurement/drain window, so it checks correctness without
    # contaminating resource, latency or throughput comparisons.
    restarted_at = utc_now()
    kubectl("rollout", "restart", "deployment/streamscale-consumer")
    kubectl("rollout", "status", "deployment/streamscale-consumer", "--timeout=180s")
    after = inspect("/runs/" + info["run_name"])
    if after is None:
        raise RuntimeError("Controlled-restart reconciliation failed")
    write_json(
        destination / "restart.json",
        {"started_at": restarted_at, "ended_at": utc_now(), "inspection": after},
    )
    row = summarize(run, measurements, samples, before, after, configuration)
    if decisions is not None:
        controller = decisions["controller"]
        row["autoscaler_policy"] = controller.get("policy")
        row["autoscaler_model_sha256"] = controller.get("model_sha256", "")
        row["autoscaler_variant"] = controller.get("variant", "full")
        row["autoscaler_decisions"] = sum(
            item["run_id"] == run["run_id"] for item in decisions["decisions"]
        )
    row["image_digest"] = metadata["image_id"]
    row["cell_key"] = cell["key"]
    row["artifact_directory"] = str(destination.relative_to(ROOT))
    write_json(destination / "summary.json", row)
    checksums = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in destination.iterdir()
        if path.is_file()
    }
    write_json(destination / "checksums.json", checksums)
    (campaign / "active-run.json").unlink()
    print(
        f"Completed {run['run_id']}: {row['status']}, facts={row['unique_facts']}, "
        f"p95={row['p95_end_to_end_seconds']}s",
        flush=True,
    )
    return row


def verify_evidence(campaign, rows):
    """Refuse reuse when preserved raw evidence has been changed or lost."""
    for row in rows:
        directory = ROOT / row["artifact_directory"]
        if directory.parent != campaign:
            raise RuntimeError("Run evidence is outside this campaign")
        checksums = json.loads((directory / "checksums.json").read_text())
        required = {
            "run.json",
            "manifest.jsonl",
            "acknowledgements.jsonl",
            "samples.jsonl",
            "sql-measurements.json",
            "before-restart-inspection.json",
            "restart.json",
            "summary.json",
        }
        if not required <= checksums.keys():
            raise RuntimeError("Incomplete raw evidence checksum manifest")
        for name, checksum in checksums.items():
            if (
                Path(name).name != name
                or hashlib.sha256((directory / name).read_bytes()).hexdigest() != checksum
            ):
                raise RuntimeError(f"Raw evidence checksum mismatch: {directory / name}")
        if json.loads((directory / "summary.json").read_text()) != row:
            raise RuntimeError("Campaign summary differs from preserved run evidence")


def validate_matrix(state):
    measured = [row for row in state["rows"] if row["phase"] == "measured"]
    expected = {cell["key"] for cell in state["plan"]}
    if len(measured) != len(expected) or {row["cell_key"] for row in measured} != expected:
        raise RuntimeError(f"The complete {len(expected)}-cell measured matrix is required")
    if any(not row["performance_eligible"] for row in measured):
        raise RuntimeError("A failed measured run cannot enter the performance comparison")
    if len({row["run_id"] for row in state["rows"]}) != len(state["rows"]):
        raise RuntimeError("Executions must have distinct run IDs")
    if {row["image_digest"] for row in state["rows"]} != {state["image"]["image_id"]}:
        raise RuntimeError("The consumer image changed during the campaign")
    # v2 compares one logical sequence per scenario; v3 pairs modes within each repetition.
    paired = state["protocol"]["protocol_version"] >= 3
    groups = {}
    for row in measured:
        key = (row["scenario"], row.get("repetition")) if paired else row["scenario"]
        groups.setdefault(key, set()).add(row["logical_sha256"])
    if any(len(values) != 1 for values in groups.values()):
        raise RuntimeError("Compared scenario workloads differ logically")


def export_csv(campaign, state):
    rows = state["rows"]
    for name, selected in (
        ("all-runs.csv", rows + state["failures"]),
        ("summary.csv", [row for row in rows if row["phase"] == "measured"]),
        (
            "comparison.csv",
            [row for row in rows if row["phase"] == "measured" and row["performance_eligible"]],
        ),
        ("failed-attempts.csv", state["failures"]),
    ):
        fields = sorted({key for row in selected for key in row}) or ["failure_reason"]
        with (campaign / name).open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            writer.writerows(selected)
    write_json(campaign / "campaign.json", state)


def environment():
    return {
        "captured_at": utc_now(),
        "docker": json.loads(
            call(["docker", "info", "--format", "{{json .}}"], capture=True).stdout
        ),
        "nodes": json.loads(kubectl("get", "nodes", "-o", "json", capture=True).stdout),
        "pods": json.loads(kubectl("get", "pods", "-o", "json", capture=True).stdout),
        "helm_values": json.loads(
            call(
                [
                    TOOLS / "helm",
                    "get",
                    "values",
                    "streamscale",
                    "--all",
                    "-n",
                    NAMESPACE,
                    "--kube-context",
                    f"kind-{CLUSTER}",
                    "-o",
                    "json",
                ],
                capture=True,
            ).stdout
        ),
    }


def configuration(protocol, phase, schedule, repetition=0):
    return {
        "protocol_version": protocol["protocol_version"],
        "phase": phase,
        "repetition": repetition,
        "schedule": schedule,
        "profile": "correctness" if phase == "measured" else "normal",
        "paced": phase != "pilot",
        "sample_interval_seconds": protocol["sample_interval_seconds"],
        "resource_interval_seconds": protocol["resource_interval_seconds"],
        "drain_timeout_seconds": protocol["drain_timeout_seconds"],
    }


def recover_active(campaign, state):
    active_file = campaign / "active-run.json"
    if not active_file.exists():
        return
    active = json.loads(active_file.read_text())
    directory = Path(active["directory"])
    config = json.loads((directory / "configuration.json").read_text())
    cell = active["cell"]
    args = deploy_arguments(cell, config)
    print("Recovering the interrupted execution before any new publication", flush=True)
    deploy(state["image"], args, wait=False, run_name=active["deployment"]["run_name"])
    deadline = time.monotonic() + config["drain_timeout_seconds"] + 180
    while time.monotonic() < deadline:
        result = inspect("/runs/" + active["deployment"]["run_name"])
        if result and correct(result):
            copy_run(active["deployment"]["run_name"], directory)
            write_json(directory / "recovery-inspection.json", result)
            failure = {
                "run_name": active["deployment"]["run_name"],
                "cell_key": cell["key"],
                "phase": config["phase"],
                "mode": cell["mode"],
                "scenario": cell["scenario"],
                "performance_eligible": 0,
                "failure_reason": "interrupted measurement; recovered and excluded",
                "artifact_directory": str(directory.relative_to(ROOT)),
                "recovered_at": utc_now(),
            }
            if not any(row.get("run_name") == failure["run_name"] for row in state["failures"]):
                state["failures"].append(failure)
            active_file.unlink()
            export_csv(campaign, state)
            return
        time.sleep(2)
    raise RuntimeError(
        "Interrupted execution has not reconciled; its evidence and consumers are retained"
    )


def record_capture(campaign, state, cell, config):
    try:
        row = capture(state["image"], cell, config, campaign)
        if not row["performance_eligible"]:
            state["failures"].append(row)
            export_csv(campaign, state)
            raise RuntimeError("Run failed measurement eligibility: " + row["failure_reason"])
        state["rows"].append(row)
        export_csv(campaign, state)
        return row
    except Exception as error:
        state["status"] = "interrupted"
        write_json(
            campaign / "last-error.json",
            {"at": utc_now(), "cell": cell, "phase": config["phase"], "error": str(error)},
        )
        export_csv(campaign, state)
        raise


def restore_demo(metadata):
    deploy(
        metadata,
        SimpleNamespace(
            mode="fixed",
            scenario="steady",
            seed=42,
            orders=200,
            profile="correctness",
            paced=False,
            start_delay=0,
        ),
    )
    from verify import main as verify_demo

    verify_demo()


def measured_cell(frozen, cell):
    """Seeded cell and measured configuration for one plan entry."""
    scenario = frozen["scenarios"][cell["scenario"]]
    if frozen["protocol_version"] < 3:
        cell = cell | {"seed": scenario["seed"]}
        config = configuration(frozen, "measured", scenario["schedule"], cell["repetition"])
        config["load_transition_seconds"] = 15 if cell["scenario"] == "flash-sale" else 0
        if cell["scenario"] == "overload-recovery":
            config["recovery_transition_seconds"] = 35
        return cell, config
    repetition = scenario["repetitions"][str(cell["repetition"])]
    cell = cell | {"seed": repetition["event_seed"], "shape_seed": repetition["shape_seed"]}
    config = configuration(frozen, "measured", repetition["schedule"], cell["repetition"])
    config["shape_seed"] = repetition["shape_seed"]
    config["sale_starts"] = repetition["sale_starts"]
    config["load_transition_seconds"] = repetition["load_transition_seconds"]
    if repetition["recovery_transition_seconds"] is not None:
        config["recovery_transition_seconds"] = repetition["recovery_transition_seconds"]
    config["autoscaler"] = frozen["autoscaler"]
    config["model_sha256"] = frozen["model"]["sha256"]
    return cell, config


def warmup_configuration(frozen, cell):
    config = configuration(
        frozen,
        "warmup",
        [[frozen["warmup_seconds"], frozen["rates"]["steady"]]],
        cell["repetition"],
    )
    if "autoscaler" in frozen:
        config["autoscaler"] = frozen["autoscaler"]
    return config


def run_campaign(args):
    name = getattr(args, "protocol", "v2")
    protocol = load_protocol(name)
    calibration = bool(getattr(args, "calibration", False))
    scratch = bool(getattr(args, "modes", None) or getattr(args, "scenarios", None))
    scratch = scratch or bool(getattr(args, "repetitions", None)) or calibration
    destination = scratch_destination(name) if scratch else DESTINATIONS[name]
    if calibration:
        # Simulator calibration runs on training-namespace shapes, never evaluation ones.
        protocol = protocol | {"shape_seed_namespace": "train"}
        destination = DESTINATIONS[name] / "calibration"
    variant = getattr(args, "variant", None)
    if variant:
        # Follow-up ablation of ml-predictive: same frozen protocol, model and seeds, with
        # one switch changed. Never reported as the M16 campaign.
        protocol = protocol | {"autoscaler": protocol["autoscaler"] | {"variant": variant}}
        destination = DESTINATIONS[name] / "followup" / variant
        scratch = True
    latest = destination / "latest.path"
    current = fingerprint()
    destination.mkdir(parents=True, exist_ok=True)
    if args.resume and latest.exists():
        campaign = Path(latest.read_text().strip())
        state = json.loads((campaign / "campaign.json").read_text())
        verify_evidence(campaign, state["rows"])
        if state["status"] == "complete":
            # Completed evidence is checksum-verified; later code changes cannot alter it.
            validate_matrix(state)
            print(f"Verified retained {len(state['plan'])}-run matrix: {campaign}", flush=True)
            return
        if state["fingerprint"] != current:
            raise RuntimeError(
                "Experiment code/configuration changed since this campaign started. Retain it "
                "and start a new one with --fresh"
            )
        cluster()
        # Preserve the exact original image even when another demo has built a newer tag.
        call(["docker", "tag", state["image"]["image_id"], state["image"]["image"]])
    else:
        cells = plan(protocol)
        if scratch:
            cells = select(cells, args.modes, args.scenarios, args.repetitions)
            if not cells:
                raise RuntimeError("The scratch selection contains no cells")
        metadata = build()
        cluster()
        campaign = destination / (
            "campaign-" + utc_now()[:19].replace(":", "").replace("-", "") + "-" + uuid4().hex[:6]
        )
        campaign.mkdir()
        state = {
            "started_at": utc_now(),
            "status": "running",
            "fingerprint": current,
            "protocol": protocol,
            "protocol_name": name,
            # Scratch campaigns are subsets for checking; analysis refuses to report them.
            "scratch": scratch,
            "purpose": "calibration"
            if calibration
            else ("followup" if variant else ("scratch" if scratch else "evidence")),
            "variant": variant or "full",
            "plan": cells,
            "image": metadata,
            "rows": [],
            "failures": [],
        }
        write_json(campaign / "campaign.json", state)
        latest.write_text(str(campaign) + "\n")
    call([TOOLS / "kind", "load", "docker-image", state["image"]["image"], "--name", CLUSTER])
    install_addons()
    if not (campaign / "environment.json").exists():
        # Limit host metadata to reproducibility fields, excluding registry/proxy/user details.
        snapshot = environment()
        snapshot["docker"] = {
            key: snapshot["docker"].get(key)
            for key in ("ServerVersion", "NCPU", "MemTotal", "Architecture", "OperatingSystem")
        }
        write_json(campaign / "environment.json", snapshot)
    recover_active(campaign, state)
    frozen_path = campaign / "frozen-protocol.json"
    if frozen_path.exists():
        frozen = json.loads(frozen_path.read_text())
    else:
        pilot_cell = {"mode": "fixed", "scenario": "steady", "seed": 99, "key": "pilot"}
        if not any(row["cell_key"] == "pilot-warmup" for row in state["rows"]):
            record_capture(
                campaign,
                state,
                pilot_cell | {"key": "pilot-warmup"},
                configuration(protocol, "warmup", [[protocol["warmup_seconds"], 30]]),
            )
        pilot = next((row for row in state["rows"] if row["phase"] == "pilot"), None)
        if pilot is None:
            pilot = record_capture(
                campaign,
                state,
                pilot_cell,
                configuration(protocol, "pilot", [[10, protocol["pilot_orders"] // 5]]),
            )
        frozen = freeze(protocol, pilot["throughput_processing_per_second"])
        frozen["pilot_run_id"] = pilot["run_id"]
        write_json(frozen_path, frozen)
    total = len(state["plan"])
    print(
        f"Frozen protocol {frozen['protocol_version']}: rates={frozen['rates']}; "
        f"{total} measured executions",
        flush=True,
    )
    completed = {row["cell_key"] for row in state["rows"] if row["performance_eligible"]}
    for cell in state["plan"]:
        if cell["key"] in completed:
            continue
        cell, config = measured_cell(frozen, cell)
        warmup = record_capture(
            campaign,
            state,
            cell | {"key": cell["key"] + "-warmup"},
            warmup_configuration(frozen, cell),
        )
        config["warmup_run_id"] = warmup["run_id"]
        config["warmup_seconds"] = frozen["warmup_seconds"]
        record_capture(campaign, state, cell, config)
        count = sum(
            row["phase"] == "measured" and row["performance_eligible"] for row in state["rows"]
        )
        print(f"Matrix progress: {count}/{total} correct measured runs", flush=True)
    validate_matrix(state)
    verify_evidence(campaign, state["rows"])
    state["status"] = "complete"
    state["ended_at"] = utc_now()
    export_csv(campaign, state)
    restore_demo(state["image"])
    label = "M16" if protocol["protocol_version"] >= 3 else "M7"
    print(
        f"{label} {'scratch ' if scratch else ''}complete: {campaign / 'summary.csv'}", flush=True
    )


def dry_run(name, capacity=None):
    """Print the plan and frozen schedules for an assumed pilot capacity; no cluster use."""
    protocol = load_protocol(name)
    if capacity is None:
        capacity = json.loads((ROOT / "docs/results/current/frozen-protocol.json").read_text())[
            "pilot_capacity_per_second"
        ]
    frozen = freeze(protocol, capacity)
    cells = plan(protocol)
    print(json.dumps({"protocol": name, "cells": len(cells), "rates": frozen["rates"]}))
    for cell in cells:
        seeded, config = measured_cell(frozen, cell)
        seconds = sum(phase[0] for phase in config["schedule"])
        print(
            f"{seeded['key']:32} seed={seeded['seed']:<6} "
            f"shape={seeded.get('shape_seed', '-'):10} {seconds:4d}s "
            f"sales@{config.get('sale_starts', '-')}"
        )
    return frozen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--resume", action="store_true", help="Reuse complete evidence or resume its frozen matrix"
    )
    group.add_argument(
        "--fresh", action="store_true", help="Create a new campaign, retaining prior evidence"
    )
    parser.add_argument("--protocol", choices=sorted(PROTOCOLS), default="v2")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan; no cluster use")
    parser.add_argument("--capacity", type=float, help="Assumed pilot capacity for --dry-run")
    scratch = parser.add_argument_group("scratch subset (never reported)")
    scratch.add_argument("--modes", type=lambda value: set(value.split(",")))
    scratch.add_argument("--scenarios", type=lambda value: set(value.split(",")))
    scratch.add_argument("--repetitions", type=int)
    scratch.add_argument(
        "--calibration",
        action="store_true",
        help="Simulator calibration subset on train: shape seeds (protocol v3 only)",
    )
    scratch.add_argument(
        "--variant",
        choices=("full", "forecast-only", "adapt-only"),
        help="Follow-up ablation of ml-predictive (protocol v3 only)",
    )
    args = parser.parse_args()
    if args.variant and args.protocol != "v3":
        parser.error("--variant needs --protocol v3")
    if args.calibration and args.protocol != "v3":
        parser.error("--calibration needs --protocol v3")
    args.resume = not args.fresh
    if args.dry_run:
        dry_run(args.protocol, args.capacity)
        return
    protocol = load_protocol(args.protocol)
    unknown = (args.modes or set()) - set(protocol_modes(protocol))
    unknown |= (args.scenarios or set()) - set(protocol["scenarios"])
    if unknown:
        parser.error(f"Not in protocol {args.protocol}: {', '.join(sorted(unknown))}")
    with workflow():
        run_campaign(args)


if __name__ == "__main__":
    main()

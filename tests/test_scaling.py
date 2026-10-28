"""Rendered controller isolation and execution-group alignment."""

import subprocess
from pathlib import Path

import pytest
from streamscale_workload.generator import generate

ROOT = Path(__file__).resolve().parents[1]
HELM = ROOT / ".tools/bin/helm"


MODES = ["fixed", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive"]


def render(mode, *extra):
    if not HELM.exists():
        pytest.skip("Install pinned Helm with make k8s-tools for chart checks")
    return subprocess.run(
        [
            str(HELM),
            "template",
            "streamscale",
            str(ROOT / "deploy/k8s/chart"),
            "--set",
            f"scaling.mode={mode}",
            "--set",
            "run.name=unique-run",
            *extra,
        ],
        text=True,
        capture_output=True,
        check=True,
    ).stdout


def documents(rendered, kind, name):
    """Rendered manifest texts of one kind and metadata name."""
    return [
        document
        for document in rendered.split("\n---")
        if f"\nkind: {kind}\n" in "\n" + document and f"\n  name: {name}\n" in document
    ]


@pytest.mark.parametrize("mode", MODES)
def test_chart_installs_only_its_selected_controller(mode):
    rendered = render(mode)
    assert rendered.count("kind: HorizontalPodAutoscaler") == (mode == "cpu-hpa")
    assert rendered.count("kind: ScaledObject") == (mode == "lag-keda")
    autoscalers = documents(rendered, "Deployment", "streamscale-autoscaler")
    assert len(autoscalers) == (mode.startswith("ml-"))
    assert f"--mode {mode} --group-id streamscale.run.unique-run" in rendered
    consumer = next(
        document
        for document in rendered.split("---")
        if "kind: Deployment" in document and "name: streamscale-consumer" in document
    )
    assert ("  replicas: 1" in consumer) == (mode == "fixed")
    if mode == "lag-keda":
        assert "bootstrapServers: streamscale-kafka.default.svc.cluster.local:9092" in rendered
        assert "consumerGroup: streamscale.run.unique-run" in rendered
        assert "minReplicaCount: 1" in rendered and "maxReplicaCount: 3" in rendered
    elif mode == "cpu-hpa":
        assert "minReplicas: 1" in rendered and "maxReplicas: 3" in rendered
    assert "INTERNAL://streamscale-kafka.default.svc.cluster.local:9092" in rendered


@pytest.mark.parametrize(
    "setting", ["consumer.replicas=2", "scaling.maxReplicas=4", "scaling.mode=both"]
)
def test_chart_rejects_out_of_contract_scaling(setting):
    if not HELM.exists():
        pytest.skip("Install pinned Helm with make k8s-tools for chart checks")
    result = subprocess.run(
        [str(HELM), "template", "streamscale", str(ROOT / "deploy/k8s/chart"), "--set", setting],
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0


@pytest.mark.parametrize("mode", ["ml-reactive", "ml-predictive"])
def test_ml_controller_cannot_read_the_schedule_and_only_scales_the_consumer(mode):
    # manage.py passes frozen numbers with --set-json; --set would keep decimals as strings.
    rendered = render(mode, "--set-json", "autoscaler.capacity=128.25")
    (controller,) = documents(rendered, "Deployment", "streamscale-autoscaler")
    assert f"- {mode.removeprefix('ml-')}\n" in controller
    assert '--capacity\n            - "128.25"' in controller
    assert "http://streamscale-exporter:9100/metrics" in controller
    # No runs volume, run ConfigMap, or database credentials.
    for forbidden in ("volumeMounts", "volumes:", "streamscale-run", "DATABASE_URL", "env:"):
        assert forbidden not in controller
    (role,) = documents(rendered, "Role", "streamscale-autoscaler")
    assert role.count("resourceNames: [streamscale-consumer]") == 2
    assert "resources: [deployments/scale]" in role and "verbs: [get, patch]" in role
    assert "resources: [deployments]" in role and "verbs: [get]" in role
    for broad in ("pods", "secrets", "configmaps", "create", "delete", "*"):
        assert broad not in role
    consumer = next(
        document
        for document in rendered.split("---")
        if "kind: Deployment" in document and "name: streamscale-consumer" in document
    )
    assert "  replicas: 1" not in consumer


def test_chart_rejects_unknown_v3_scenarios():
    if not HELM.exists():
        pytest.skip("Install pinned Helm with make k8s-tools for chart checks")
    for scenario, accepted in (("ramp", True), ("repeated-sales", True), ("black-friday", False)):
        result = subprocess.run(
            [
                str(HELM),
                "template",
                "streamscale",
                str(ROOT / "deploy/k8s/chart"),
                "--set",
                f"run.scenario={scenario}",
            ],
            text=True,
            capture_output=True,
        )
        assert (result.returncode == 0) == accepted


def test_scaler_and_consumer_share_the_explicit_unique_run_group(tmp_path):
    run = generate(tmp_path / "run", orders=2, mode="lag-keda", group_id="streamscale.run.unique")
    assert run["group_id"] == "streamscale.run.unique"
    assert run["mode"] == "lag-keda"


def test_empty_group_is_rejected_before_creating_artifacts(tmp_path):
    with pytest.raises(ValueError, match="Consumer group"):
        generate(tmp_path / "run", group_id=" ")
    assert not (tmp_path / "run").exists()

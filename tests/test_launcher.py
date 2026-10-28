"""Exercise scoped lifecycle commands without stopping the user's actual services."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FAKE = """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["STREAMSCALE_TEST_LOG"], "a") as out:
    out.write(json.dumps([name, *args]) + "\\n")
if name == "docker":
    if args[:1] == ["info"] and os.environ.get("STREAMSCALE_TEST_DOCKER_DOWN"):
        sys.exit(1)
    if args[:1] == ["ps"]:
        if "label=com.docker.compose.project=streamscale-local" in args:
            print("streamscale-local-kafka-1\\nstreamscale-local-postgres-1")
        elif "label=io.x-k8s.kind.cluster=streamscale" in args:
            print("streamscale-control-plane")
        else:
            sys.exit("Unscoped Docker container query")
if name == "make" and os.environ.get("STREAMSCALE_TEST_FAIL_MAKE") in args:
    sys.exit(9)
"""


@pytest.fixture
def launcher(tmp_path):
    checkout = tmp_path / "checkout with spaces"
    checkout.mkdir()
    shutil.copy2(ROOT / "streamscale.sh", checkout / "streamscale.sh")
    (checkout / ".env.example").write_text("DATABASE_URL=example\n")
    (checkout / ".env").write_text("PRESERVE_ME=yes\n")
    binaries = tmp_path / "bin"
    binaries.mkdir()
    for name in ("docker", "make", "uv", "node", "npm"):
        binary = binaries / name
        binary.write_text(FAKE)
        binary.chmod(0o755)
    tools = checkout / ".tools/bin"
    tools.mkdir(parents=True)
    (tools / "kubectl").write_text(FAKE)
    (tools / "kubectl").chmod(0o755)
    (checkout / "artifacts").mkdir()
    (checkout / "artifacts/kubeconfig").write_text("test only\n")
    log = tmp_path / "calls.jsonl"
    environment = os.environ | {
        "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
        "STREAMSCALE_TEST_LOG": str(log),
    }

    def run(*arguments, **overrides):
        # Invoke from outside the checkout: the launcher must find its own root.
        result = subprocess.run(
            ["bash", str(checkout / "streamscale.sh"), *arguments],
            cwd=tmp_path,
            env=environment | overrides,
            capture_output=True,
            text=True,
            timeout=15,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return result, calls, checkout

    return run


def test_stop_targets_only_project_containers_and_retains_all_data(launcher):
    result, calls, _ = launcher("stop")
    assert result.returncode == 0, result.stderr
    stops = [call for call in calls if call[:2] == ["docker", "stop"]]
    assert stops == [
        [
            "docker",
            "stop",
            "--timeout",
            "45",
            "streamscale-local-kafka-1",
            "streamscale-local-postgres-1",
        ],
        ["docker", "stop", "--timeout", "45", "streamscale-control-plane"],
    ]
    assert not any(call[0] in {"uv", "make"} for call in calls)
    assert not any(
        argument in {"rm", "prune", "delete", "--volumes", "-v"}
        for call in calls
        for argument in call
    )


@pytest.mark.parametrize(
    ("command", "expected"),
    [
        ("stop-local", ["streamscale-local-kafka-1", "streamscale-local-postgres-1"]),
        ("stop-cluster", ["streamscale-control-plane"]),
    ],
)
def test_partial_shutdown_leaves_other_stack_untouched(launcher, command, expected):
    result, calls, _ = launcher(command)
    assert result.returncode == 0, result.stderr
    stops = [call for call in calls if call[:2] == ["docker", "stop"]]
    assert len(stops) == 1
    assert stops[0][4:] == expected


def test_complete_flow_preserves_env_uses_fresh_run_and_executes_current_milestones(launcher):
    result, calls, checkout = launcher("all", "--no-ui")
    assert result.returncode == 0, result.stderr
    assert (checkout / ".env").read_text() == "PRESERVE_ME=yes\n"
    targets = [call[1] for call in calls if call[0] == "make"]
    assert targets == [
        "up",
        "bootstrap",
        "smoke",
        "demo",
        "test",
        "check",
        "integration",
        "ml-test",
        "ml-eval",
        "k8s-up",
        "k8s-verify",
        "scaling",
        "experiments-v3",
        "analysis-v3",
        "console-check",
    ]
    # M7's v2 campaign and report are not rerun or rewritten by all.
    assert "experiments" not in targets and "analysis" not in targets
    directory = (checkout / "artifacts/launcher/last-local-run.path").read_text().strip()
    assert Path(directory).parent.is_dir()
    assert ["make", "demo", f"RUN_DIR={directory}"] in calls
    assert not any("deploy/ui.py" in argument for call in calls for argument in call)
    assert not any("serve" in call for call in calls if call[0] == "uv")
    # The generated directory itself must not exist before the generator runs.
    assert not Path(directory).exists()


def test_resume_waits_for_api_and_rollouts_without_publishing(launcher):
    result, calls, _ = launcher("resume")
    assert result.returncode == 0, result.stderr
    targets = [call[1] for call in calls if call[0] == "make"]
    assert targets == ["up", "bootstrap"]
    assert ["docker", "start", "streamscale-control-plane"] in calls
    assert any("--raw=/readyz" in call for call in calls)
    rollouts = [call for call in calls if "rollout" in call]
    # Six pipeline rollouts, plus the console release's two when it is installed.
    assert len(rollouts) == 8
    assert all("kind-streamscale" in call and "streamscale" in call for call in rollouts)
    assert ["deployment/streamscale-console-agent"] == [
        argument for argument in rollouts[6] if argument.startswith("deployment/")
    ]


def test_failed_stage_stops_before_cluster_deployment(launcher):
    result, calls, _ = launcher("all", "--no-ui", STREAMSCALE_TEST_FAIL_MAKE="test")
    assert result.returncode != 0
    assert not any("k8s-up" in call for call in calls)


def test_docker_unavailable_is_actionable_and_never_stops_anything(launcher):
    result, calls, _ = launcher("stop", STREAMSCALE_TEST_DOCKER_DOWN="1")
    assert result.returncode != 0
    assert "Start Docker" in result.stderr
    assert not any("stop" in call for call in calls)


CONSOLE_SERVE = [
    "uv",
    "run",
    "--project",
    "console",
    "--frozen",
    "python",
    "-m",
    "streamscale_console",
    "serve",
    "--dashboards",
]


@pytest.mark.parametrize(
    ("command", "entry"),
    [
        ("ui", ["uv", "run", "--frozen", "python", "deploy/ui.py"]),
        ("all", CONSOLE_SERVE),
        ("console", CONSOLE_SERVE),
    ],
)
def test_dashboard_ports_reach_the_ui_entry_point(launcher, command, entry):
    result, calls, _ = launcher(command, "--grafana-port", "13001", "--prometheus-port", "0")
    assert result.returncode == 0, result.stderr
    assert calls[-1] == [*entry, "--grafana-port", "13001", "--prometheus-port", "0"]


def test_console_builds_deploys_and_serves_on_the_requested_port(launcher):
    result, calls, _ = launcher("console", "--port", "18081")
    assert result.returncode == 0, result.stderr
    assert ["make", "console-web", "console-deploy"] in calls
    assert ["docker", "start", "streamscale-control-plane"] in calls
    assert calls[-1] == [*CONSOLE_SERVE, "--port", "18081"]
    assert not any(call[:2] == ["docker", "stop"] for call in calls)


def test_console_check_runs_the_console_acceptance_target(launcher):
    result, calls, _ = launcher("console-check")
    assert result.returncode == 0, result.stderr
    assert [call[1] for call in calls if call[0] == "make"] == ["console-check"]


@pytest.mark.parametrize(
    "arguments",
    [
        ("ui", "--grafana-port"),
        ("ui", "--grafana-port", "65536"),
        ("all", "--grafana-port", "-1"),
        ("all", "--prometheus-port", "not-a-port"),
        ("all", "--no-ui", "--grafana-port", "0"),
        ("verify", "--grafana-port", "13001"),
        ("all", "--port", "18081"),
        ("console", "--port", "70000"),
        ("console", "--port"),
    ],
)
def test_invalid_ui_options_fail_before_services_or_publication(launcher, arguments):
    result, calls, _ = launcher(*arguments)
    assert result.returncode == 2
    assert calls == []


def test_m16_campaign_has_its_own_fresh_and_analysis_commands(launcher):
    result, calls, _ = launcher("experiments-v3", "--fresh")
    assert result.returncode == 0, result.stderr
    assert [call[1] for call in calls if call[0] == "make"] == ["experiments-v3-fresh"]
    seen = len(calls)
    result, calls, _ = launcher("analysis-v3")
    assert result.returncode == 0, result.stderr
    # The fake-call log accumulates across invocations in one test.
    assert [call[1] for call in calls[seen:] if call[0] == "make"] == ["analysis-v3"]
    result, _, _ = launcher("cluster", "--fresh")
    assert result.returncode == 2
    assert "experiments or experiments-v3" in result.stderr

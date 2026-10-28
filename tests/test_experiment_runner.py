"""Experiment order, calibration and tampered evidence must not silently change."""

import hashlib
import json

import pytest

from experiments.metrics import sale_metrics
from experiments.run import (
    ROOT,
    configuration,
    freeze,
    load_protocol,
    measured_cell,
    plan,
    protocol_modes,
    select,
    validate_matrix,
    verify_evidence,
)


def test_sale_metrics_count_readiness_responses_and_changes():
    samples = [
        {"time": 100 + second, "ready": ready, "desired": desired}
        for second, (ready, desired) in enumerate(
            [(1, 1)] * 15 + [(1, 3)] * 4 + [(3, 3)] * 30 + [(1, 1)] * 10 + [(3, 3)] * 10
        )
    ]
    result = sale_metrics(samples, 100, [15, 60])
    assert result["sales"] == 2
    assert result["mean_ready_at_sale_start"] == 2.0
    assert result["min_ready_at_sale_start"] == 1
    assert result["mean_sale_response_seconds"] == 4
    assert result["scale_changes"] == 3
    # Warmups and steady runs have no sale; that must not fail the capture.
    quiet = sale_metrics(samples, 100, [])
    assert quiet["sales"] == 0 and quiet["mean_ready_at_sale_start"] is None


def test_exporter_waits_for_successful_startup_probe(monkeypatch):
    from types import SimpleNamespace

    import experiments.run as runner

    responses = []
    for started in (False, True):
        responses.append(
            SimpleNamespace(
                stdout=json.dumps(
                    {
                        "items": [
                            {
                                "metadata": {
                                    "name": "exporter",
                                    "annotations": {"streamscale/run-name": "run"},
                                },
                                "status": {
                                    "phase": "Running",
                                    "containerStatuses": [{"started": started}],
                                },
                            }
                        ]
                    }
                )
            )
        )
    monkeypatch.setattr(runner, "kubectl", lambda *args, **kwargs: responses.pop(0))
    monkeypatch.setattr(runner.time, "sleep", lambda _: None)
    assert runner.current_exporter("run") == "exporter"
    assert not responses


def test_v3_plan_pairs_all_five_modes_on_reserved_seeds():
    protocol = load_protocol("v3")
    cells = plan(protocol)
    assert len(cells) == len({cell["key"] for cell in cells}) == 75
    assert cells == plan(protocol)
    frozen = freeze(protocol, 130.497)
    assert frozen["rates"] == {"steady": 52, "surge": 390}
    assert frozen["model"]["training_seeds"].startswith("train:")
    assert frozen["autoscaler"]["capacity"] == 130.497
    seeds = set()
    for cell in cells:
        seeded, config = measured_cell(frozen, cell)
        assert seeded["shape_seed"].startswith("eval:")
        assert config["model_sha256"] == frozen["model"]["sha256"]
        seeds.add((cell["scenario"], cell["repetition"], seeded["seed"], seeded["shape_seed"]))
    # One seed pair per (scenario, repetition), shared by every mode.
    assert len(seeds) == 15
    repeated = frozen["scenarios"]["repeated-sales"]["repetitions"]
    assert len({json.dumps(item["schedule"]) for item in repeated.values()}) == 3
    overload = frozen["scenarios"]["overload-recovery"]["repetitions"]["1"]
    assert overload["recovery_transition_seconds"] == 35
    subset = select(cells, {"lag-keda", "ml-predictive"}, {"ramp", "repeated-sales"}, 1)
    assert len(subset) == 4


def test_v2_plan_is_unchanged_by_m16():
    protocol = load_protocol("v2")
    assert protocol_modes(protocol) == ("fixed", "cpu-hpa", "lag-keda")
    frozen = freeze(protocol, 130.497)
    cell, config = measured_cell(frozen, plan(protocol)[0])
    assert "autoscaler" not in config and "shape_seed" not in cell


def test_plan_has_every_cell_once_in_seeded_order():
    protocol = json.loads((ROOT / "experiments/protocol.json").read_text())
    cells = plan(protocol)
    assert cells == plan(protocol)
    assert len(cells) == len({cell["key"] for cell in cells}) == 27
    assert all(
        sum(cell["mode"] == mode for cell in cells) == 9
        for mode in ("fixed", "cpu-hpa", "lag-keda")
    )
    frozen = freeze(protocol, 91.5)
    assert frozen["rates"] == {"steady": 36, "surge": 274}
    assert all(
        sum(seconds for seconds, _ in scenario["schedule"]) == 60
        for scenario in frozen["scenarios"].values()
    )
    assert "rates" not in protocol
    assert configuration(frozen, "measured", [[60, 36]])["protocol_version"] == 2


def test_matrix_refuses_missing_failed_or_different_workloads():
    protocol = json.loads((ROOT / "experiments/protocol.json").read_text())
    cells = plan(protocol)
    rows = [
        dict(
            cell_key=cell["key"],
            phase="measured",
            performance_eligible=1,
            run_id=str(index),
            image_digest="same",
            logical_sha256=cell["scenario"],
            scenario=cell["scenario"],
        )
        for index, cell in enumerate(cells)
    ]
    state = dict(rows=rows, plan=cells, image={"image_id": "same"}, protocol=protocol)
    validate_matrix(state)
    rows[0]["performance_eligible"] = 0
    with pytest.raises(RuntimeError, match="failed"):
        validate_matrix(state)
    rows[0]["performance_eligible"] = 1
    rows[0]["logical_sha256"] = "different"
    with pytest.raises(RuntimeError, match="differ logically"):
        validate_matrix(state)


def test_reuse_refuses_changed_raw_evidence(tmp_path):
    directory = tmp_path / "campaign/run"
    directory.mkdir(parents=True)
    row = {"artifact_directory": str(directory)}
    for name in (
        "run.json",
        "manifest.jsonl",
        "acknowledgements.jsonl",
        "samples.jsonl",
        "sql-measurements.json",
        "before-restart-inspection.json",
        "restart.json",
    ):
        (directory / name).write_text("{}\n")
    (directory / "summary.json").write_text(json.dumps(row))
    hashes = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in directory.iterdir()
    }
    (directory / "checksums.json").write_text(json.dumps(hashes))
    verify_evidence(directory.parent, [row])
    (directory / "samples.jsonl").write_text("tampered")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        verify_evidence(directory.parent, [row])

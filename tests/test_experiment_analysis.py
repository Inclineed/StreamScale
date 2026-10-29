"""Three repetitions are required and missing responses must remain missing."""

import pytest

from experiments.analyze import METRICS, aggregate, display
from experiments.evidence import MODES, SCENARIOS


def rows():
    return [
        {
            "mode": mode,
            "scenario": scenario,
            "repetition": repeat,
            **{metric: repeat for metric in METRICS},
        }
        for mode in MODES
        for scenario in SCENARIOS
        for repeat in range(1, 4)
    ]


def test_descriptive_aggregation_uses_per_run_values_and_reports_range():
    result = aggregate(rows())
    row = result[0]
    assert row["observed"] == 3
    assert row["median"] == 2
    assert (row["minimum"], row["maximum"]) == (1, 3)
    assert display(row) == "2.00 [1.00, 3.00]"


def test_missing_scaling_response_is_not_a_zero_second_response():
    source = rows()
    for row in source:
        row["scaling_response_seconds"] = None
    summaries = [row for row in aggregate(source) if row["metric"] == "scaling_response_seconds"]
    assert all(row["median"] is None and row["missing"] == 3 for row in summaries)
    assert display(summaries[0]) == "unobserved"


def test_incomplete_repetitions_cannot_be_ranked():
    with pytest.raises(RuntimeError, match="three distinct"):
        aggregate(rows()[1:])


def test_v3_aggregation_covers_every_mode_scenario_and_new_metric():
    from experiments.analyze import V3_METRICS

    modes = ("fixed", "cpu-hpa", "lag-keda", "ml-reactive", "ml-predictive")
    scenarios = ("steady", "ramp", "repeated-sales")
    source = [
        {
            "mode": mode,
            "scenario": scenario,
            "repetition": repeat,
            **{metric: repeat for metric in (*METRICS, *V3_METRICS)},
        }
        for mode in modes
        for scenario in scenarios
        for repeat in range(1, 4)
    ]
    result = aggregate(source, modes, scenarios, (*METRICS, *V3_METRICS))
    assert len(result) == len(modes) * len(scenarios) * (len(METRICS) + len(V3_METRICS))
    single = [row for row in source if row["repetition"] == 1]
    assert aggregate(single, modes, scenarios, METRICS, repetitions=1)[0]["observed"] == 1


def test_supplement_refuses_to_write_frozen_result_folders(monkeypatch, tmp_path):
    from experiments import supplement

    for folder in ("docs/results/m16", "docs/results/current"):
        monkeypatch.setattr(
            "sys.argv", ["supplement", "--campaign", str(tmp_path), "--output", folder]
        )
        monkeypatch.chdir(supplement.ROOT)
        with pytest.raises(SystemExit, match="never written"):
            supplement.main()


def test_predicted_and_measured_compliance_are_separate(tmp_path):
    import json

    from experiments import supplement

    directory = tmp_path / "run"
    directory.mkdir()
    (directory / "sql-measurements.json").write_text(
        json.dumps(
            {
                "facts": [
                    {"produced_at": "2026-10-29T00:00:00Z", "ingested_at": "2026-10-29T00:00:01Z"},
                    {"produced_at": "2026-10-29T00:00:00Z", "ingested_at": "2026-10-29T00:00:09Z"},
                ]
            }
        )
    )
    (directory / "autoscaler-decisions.json").write_text(
        json.dumps(
            {
                "decisions": [
                    {
                        "run_id": "r",
                        "at": 1793232001,
                        "replicas": 3,
                        "predicted_delay": {"1": 9.0, "3": 0.5},
                    },
                    {
                        "run_id": "r",
                        "at": 1793232002,
                        "replicas": 1,
                        "predicted_delay": {"1": 4.0, "3": 0.2},
                    },
                ]
            }
        )
    )
    row = {
        "run_id": "r",
        "artifact_directory": str(directory),
        "measurement_start": "2026-10-29T00:00:00Z",
        "sampling_ended_at": "2026-10-29T01:00:00Z",
    }
    assert supplement.measured_compliance(row) == 0.5
    share, middle = supplement.predicted_compliance(row)
    assert share == 0.5 and middle == 2.25

import json

import pytest
from streamscale_ml import calibrate
from streamscale_ml.policies import controller, pilot
from streamscale_ml.sim import Cluster, FixedController, KedaLike, arrivals, percentile, run
from streamscale_workload.shapes import SHAPES, sale_starts, schedule

RATES, CAPACITY = pilot()
CLUSTER = calibrate.load()


def test_correctness_profile_adds_records_and_a_late_tail():
    incoming = arrivals([[10, 100]], Cluster())
    assert len(incoming) == 20
    assert sum(incoming) == pytest.approx(1000 * 1.02)
    assert incoming[15] == pytest.approx(100 * 0.05 * 1.02)


@pytest.mark.parametrize("mode", ["fixed", "cpu-hpa", "lag-keda", "ml-reactive"])
@pytest.mark.parametrize("shape", SHAPES)
def test_every_mode_drains_without_losing_records(mode, shape):
    phases = schedule(shape, "train:0", RATES["steady"], RATES["surge"])
    result = run(phases, controller(mode, CAPACITY), CLUSTER, sale_starts(phases, 52))
    assert result.lag[-1] < 0.5
    assert min(result.lag) >= 0
    assert 1 <= result.max_ready <= 3
    assert sum(result.arrivals) == pytest.approx(sum(arrivals(phases, CLUSTER)))


def test_fixed_flash_sale_backlog_matches_the_queue_arithmetic():
    phases = [[15, 52], [20, 390], [25, 52]]
    result = run(phases, FixedController(), Cluster(capacity=130.0), [15])
    surge_growth = 20 * (390 * 1.02 * 0.95 - 130)
    assert result.peak_lag == pytest.approx(surge_growth, rel=0.15)
    assert result.max_ready == 1


def test_keda_waits_for_the_hpa_sync():
    phases = [[15, 52], [20, 390], [25, 52]]
    early = run(phases, KedaLike(), Cluster(hpa_phase=1.0), [15])
    late = run(phases, KedaLike(), Cluster(hpa_phase=14.0), [15])
    assert early.scaling_response < late.scaling_response


def test_weighted_nearest_rank_percentile():
    assert percentile([(1, 94), (10, 6)], 0.95) == 10
    assert percentile([(1, 96), (10, 4)], 0.95) == 1


def test_committed_calibration_passes_its_gate():
    stored = json.loads(calibrate.CALIBRATION.read_text())
    assert stored["failures"] == []
    targets, rates = calibrate.measured()
    assert calibrate.check(calibrate.compare(CLUSTER, targets, rates)) == []


def test_cluster_calibration_refuses_evidence_campaigns(tmp_path):
    (tmp_path / "campaign.json").write_text(json.dumps({"purpose": "evidence", "rows": []}))
    with pytest.raises(ValueError, match="Only calibration campaigns"):
        calibrate.cluster_runs(tmp_path)
    (tmp_path / "campaign.json").write_text(json.dumps({"purpose": "calibration", "rows": []}))
    (tmp_path / "frozen-protocol.json").write_text(json.dumps({"shape_seed_namespace": "eval"}))
    with pytest.raises(ValueError, match="non-training shape seeds"):
        calibrate.cluster_runs(tmp_path)


def test_three_consumer_efficiency_lowers_simulated_capacity():
    phases = [[10, 52], [30, 390], [20, 52]]
    keda = calibrate.controller("lag-keda")
    full = run(phases, keda, Cluster(efficiency_3=1.0), [10])
    slow = run(phases, calibrate.controller("lag-keda"), Cluster(efficiency_3=0.7), [10])
    assert slow.peak_lag > full.peak_lag

from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException

from streamscale.run_artifacts import write_json
from streamscale.service import kafka_sample, kafka_snapshot, wait_run


def test_backlog_comes_from_real_high_offsets_and_group_commit_positions():
    client = Mock()
    client.committed.return_value = [
        SimpleNamespace(partition=0, offset=5),
        SimpleNamespace(partition=1, offset=-1001),
        SimpleNamespace(partition=2, offset=20),
    ]
    client.get_watermark_offsets.side_effect = [(0, 10), (4, 14), (0, 20)]
    future = Mock()
    future.result.return_value = SimpleNamespace(members=["one"])
    admin = Mock()
    admin.describe_consumer_groups.return_value = {"run-group": future}
    assert kafka_snapshot(
        client, admin, {"group_id": "run-group", "start_offsets": {"0": 0, "1": 7, "2": 0}}
    ) == ({0: 5, 1: 7, 2: 0}, 1)
    client.subscribe.assert_not_called()  # Exporter never joins the processing group.


def test_sample_exposes_broker_high_watermarks_for_arrival_rates():
    client = Mock()
    client.committed.return_value = [
        SimpleNamespace(partition=0, offset=5),
        SimpleNamespace(partition=1, offset=-1001),
        SimpleNamespace(partition=2, offset=20),
    ]
    client.get_watermark_offsets.side_effect = [(0, 10), (4, 14), (0, 20)]
    future = Mock()
    future.result.return_value = SimpleNamespace(members=["one", "two"])
    admin = Mock()
    admin.describe_consumer_groups.return_value = {"run-group": future}
    lag, highs, members = kafka_sample(
        client, admin, {"group_id": "run-group", "start_offsets": {"0": 0, "1": 7, "2": 0}}
    )
    assert lag == {0: 5, 1: 7, 2: 0}
    assert highs == {0: 10, 1: 14, 2: 20}
    assert members == 2


def test_unavailable_group_metrics_do_not_fabricate_zero_replicas():
    client = Mock()
    client.committed.return_value = []
    future = Mock()
    future.result.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
    admin = Mock()
    admin.describe_consumer_groups.return_value = {"group": future}
    with pytest.raises(KafkaException):
        kafka_snapshot(client, admin, {"group_id": "group"})


def test_failed_workload_fails_waiter_immediately(tmp_path):
    write_json(tmp_path / "run.json", {"artifact_version": 1, "status": "failed"})
    with pytest.raises(RuntimeError, match="workload failed"):
        wait_run(tmp_path, Event())

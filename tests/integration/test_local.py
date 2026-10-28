import hashlib
import json
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from streamscale.assets import asset_directory
from streamscale.migrations import migrate
from streamscale.smoke import smoke
from streamscale.topics import DLQ_TOPIC, INPUT_TOPIC, ensure_topics

pytestmark = pytest.mark.integration
CASES = json.loads(
    (Path(__file__).parents[1] / "fixtures/projection-cases.v1.json").read_text(encoding="utf-8")
)


def insert_run(connection, run_id):
    connection.execute(
        """INSERT INTO experiment_run
           (run_id, mode, scenario, seed, started_at, git_commit, configuration)
           VALUES (%s, 'fixed', 'steady', 42, now(), %s, %s)""",
        (run_id, "0" * 40, Jsonb({"test": True})),
    )


def insert_event(connection, run_id, event_id, order_id, offset=0):
    connection.execute(
        """INSERT INTO order_event
           (event_id, run_id, order_id, event_type, event_time, produced_at,
            order_amount, schema_version, kafka_partition, kafka_offset, accepted_payload)
           VALUES (%s, %s, %s, 'ORDER_CREATED', now(), now(),
                   '13.37', 1, 0, %s, %s)""",
        (event_id, run_id, order_id, offset, Jsonb({"test": True})),
    )


def test_live_topics_and_repeat_bootstrap(settings):
    expected = {INPUT_TOPIC: 3, DLQ_TOPIC: 3}
    assert ensure_topics(settings.kafka_bootstrap_servers) == expected
    assert ensure_topics(settings.kafka_bootstrap_servers) == expected


def test_live_publish_consume_and_postgres(settings):
    assert migrate(settings.database_url) == []
    result = smoke(settings)
    assert result["status"] == "passed"
    assert result["published"] == result["consumed"] == 3
    assert result["postgres_decimal"] == "13.37"
    assert {coordinate["partition"] for coordinate in result["coordinates"]} == {0, 1, 2}


def test_applied_migration_is_idempotent_and_checksum_protected(scratch_database, tmp_path):
    assert migrate(scratch_database) == []
    copy_migrations(tmp_path)
    body = (asset_directory("migrations") / "0001_initial.sql").read_bytes()
    copy = tmp_path / "0001_initial.sql"
    assert migrate(scratch_database, tmp_path) == []
    with psycopg.connect(scratch_database) as connection:
        stored = connection.execute("SELECT sha256 FROM _streamscale_migration").fetchone()[0]
        assert stored == hashlib.sha256(body).hexdigest()
    copy.write_bytes(body + b"\n-- attempted rewrite\n")
    with pytest.raises(RuntimeError, match="Applied migration changed"):
        migrate(scratch_database, tmp_path)


def copy_migrations(directory):
    """Copy every real migration so a test revision matches the scratch database."""
    applied = sorted(asset_directory("migrations").glob("[0-9]*_*.sql"))
    for source in applied:
        (directory / source.name).write_bytes(source.read_bytes())
    return len(applied)


def test_failing_migration_rolls_back_ddl_and_ledger(scratch_database, tmp_path):
    existing = copy_migrations(tmp_path)
    (tmp_path / "9999_failure.sql").write_text(
        "CREATE TABLE should_rollback (id INT); SELECT * FROM nonexistent_table;"
    )
    with pytest.raises(psycopg.errors.UndefinedTable):
        migrate(scratch_database, tmp_path)
    with psycopg.connect(scratch_database) as connection:
        assert connection.execute("SELECT to_regclass('should_rollback')").fetchone()[0] is None
        ledger = connection.execute("SELECT count(*) FROM _streamscale_migration").fetchone()[0]
        assert ledger == existing


def test_event_id_is_global_and_source_coordinate_is_unique(scratch_database):
    with psycopg.connect(scratch_database) as connection:
        first, second, event, order = uuid4(), uuid4(), uuid4(), uuid4()
        insert_run(connection, first)
        insert_run(connection, second)
        insert_event(connection, first, event, order)
        with pytest.raises(psycopg.errors.UniqueViolation), connection.transaction():
            insert_event(connection, second, event, order)
        with pytest.raises(psycopg.errors.UniqueViolation), connection.transaction():
            insert_event(connection, first, uuid4(), order)
        # Reusing coordinates in a fresh run is permitted; distinct event ID remains mandatory.
        insert_event(connection, second, uuid4(), order)


@pytest.mark.parametrize("operation", ["UPDATE", "DELETE"])
def test_history_rejects_mutation(scratch_database, operation):
    with psycopg.connect(scratch_database) as connection:
        run, event, order = uuid4(), uuid4(), uuid4()
        insert_run(connection, run)
        insert_event(connection, run, event, order)
        statement = (
            "UPDATE order_event SET order_amount = 0 WHERE event_id = %s"
            if operation == "UPDATE"
            else "DELETE FROM order_event WHERE event_id = %s"
        )
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState), connection.transaction():
            connection.execute(statement, (event,))
        assert (
            connection.execute(
                "SELECT order_amount::text FROM order_event WHERE event_id = %s", (event,)
            ).fetchone()[0]
            == "13.37"
        )


def test_state_is_isolated_by_run(scratch_database):
    with psycopg.connect(scratch_database) as connection:
        order = uuid4()
        for run in (uuid4(), uuid4()):
            event = uuid4()
            insert_run(connection, run)
            insert_event(connection, run, event, order)
            connection.execute(
                """INSERT INTO order_state
                   (run_id, order_id, status, order_amount, last_event_id,
                    last_event_time, last_kafka_partition, last_kafka_offset)
                   VALUES (%s, %s, 'CREATED', '13.37', %s, now(), 0, 0)""",
                (run, order, event),
            )
        assert (
            connection.execute(
                "SELECT count(*) FROM order_state WHERE order_id = %s", (order,)
            ).fetchone()[0]
            == 2
        )


@pytest.mark.parametrize("case", CASES, ids=[case["case"] for case in CASES])
def test_ordering_rule_uses_postgres_instants(scratch_database, case):
    with psycopg.connect(scratch_database) as connection:
        result = connection.execute(
            "SELECT (%s::timestamptz, %s::bigint) > (%s::timestamptz, %s::bigint)",
            (
                case["incoming_time"],
                case["incoming_offset"],
                case["stored_time"],
                case["stored_offset"],
            ),
        ).fetchone()[0]
        assert result is case["apply"]


def test_only_planned_history_indexes_exist(scratch_database):
    with psycopg.connect(scratch_database) as connection:
        names = {
            row[0]
            for row in connection.execute(
                """SELECT indexname FROM pg_indexes
                   WHERE schemaname = current_schema() AND tablename = 'order_event'"""
            ).fetchall()
        }
        assert len(names) == 4
        assert {
            "order_event_pkey",
            "order_event_run_time_idx",
            "order_event_order_time_idx",
        } <= names

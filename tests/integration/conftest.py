from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from streamscale.config import Settings
from streamscale.migrations import migrate


@pytest.fixture(scope="module")
def settings():
    return Settings.from_env()


@pytest.fixture(scope="module")
def scratch_database(settings):
    schema = f"streamscale_test_{uuid4().hex}"
    with psycopg.connect(settings.database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    database = make_conninfo(settings.database_url, options=f"-c search_path={schema}")
    try:
        assert migrate(database) == ["0001_initial.sql", "0002_ml_modes_and_v3_scenarios.sql"]
        yield database
    finally:
        with psycopg.connect(settings.database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

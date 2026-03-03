"""Shared fixtures for integration tests.

Integration tests require a running PostgreSQL instance.
Set PGAGENT_PG_DSN env var or default to the demo compose setup.
Tests are skipped if PG is not reachable.
"""

from __future__ import annotations

import os

import pytest

DSN = os.environ.get(
    "PGAGENT_PG_DSN",
    "postgresql://pgagent:pgagent@localhost:5433/demo",
)


def pg_is_reachable() -> bool:
    try:
        import psycopg2

        conn = psycopg2.connect(DSN, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(
    not pg_is_reachable(),
    reason="PostgreSQL not reachable (start demo: cd demo && docker compose up -d)",
)


@pytest.fixture
def pg_conn():
    """Yield a psycopg2 connection, rolled back after test."""
    import psycopg2

    conn = psycopg2.connect(DSN)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def settings():
    from pgagent.config import Settings

    return Settings(pg_dsn=DSN)


@pytest.fixture
def sidecar():
    from pgagent.sidecar import SidecarDB

    db = SidecarDB(":memory:")
    yield db
    db.close()

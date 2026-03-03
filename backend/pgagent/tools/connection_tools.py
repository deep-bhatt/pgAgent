"""Connection management tools — kill idle transactions."""

from __future__ import annotations

import logging

import psycopg2

from pgagent.exceptions import SQLExecutionError

logger = logging.getLogger(__name__)


def kill_idle_transaction(conn: psycopg2.extensions.connection, sql: str) -> dict:
    """Terminate an idle-in-transaction backend using pg_terminate_backend.

    Args:
        conn: psycopg2 connection
        sql: SELECT pg_terminate_backend(pid) statement
    """
    try:
        cur = conn.cursor()
        logger.info("Executing: %s", sql)
        cur.execute(sql)
        result = cur.fetchone()
        conn.commit()
        cur.close()
        terminated = result[0] if result else False
        return {"status": "success", "terminated": terminated, "sql": sql}
    except Exception as e:
        conn.rollback()
        raise SQLExecutionError(f"Failed to kill connection: {e}") from e

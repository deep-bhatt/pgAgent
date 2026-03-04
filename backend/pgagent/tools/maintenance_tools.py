"""Maintenance tools — VACUUM, ANALYZE."""

from __future__ import annotations

import logging

import psycopg2

from pgagent.exceptions import SQLExecutionError

logger = logging.getLogger(__name__)


def vacuum_table(conn: psycopg2.extensions.connection, sql: str) -> dict:
    """Vacuum a table (requires autocommit).

    Args:
        conn: psycopg2 connection
        sql: VACUUM statement (e.g., "VACUUM users")
    """
    old_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Executing: %s", sql)
        cur.execute(sql)
        cur.close()
        return {"status": "success", "sql": sql}
    except Exception as e:
        raise SQLExecutionError(f"Failed to vacuum: {e}") from e
    finally:
        conn.autocommit = old_autocommit


def analyze_table(conn: psycopg2.extensions.connection, sql: str) -> dict:
    """Analyze a table to update statistics.

    Args:
        conn: psycopg2 connection
        sql: ANALYZE statement
    """
    try:
        cur = conn.cursor()
        logger.info("Executing: %s", sql)
        cur.execute(sql)
        conn.commit()
        cur.close()
        return {"status": "success", "sql": sql}
    except Exception as e:
        conn.rollback()
        raise SQLExecutionError(f"Failed to analyze: {e}") from e

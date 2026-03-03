"""Index management tools — CREATE/DROP INDEX CONCURRENTLY."""

from __future__ import annotations

import logging

import psycopg2

from pgagent.exceptions import SQLExecutionError

logger = logging.getLogger(__name__)


def create_index(conn: psycopg2.extensions.connection, sql: str) -> dict:
    """Create an index using CONCURRENTLY (requires autocommit).

    Args:
        conn: psycopg2 connection (will be set to autocommit)
        sql: CREATE INDEX CONCURRENTLY statement

    Returns:
        dict with execution result
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
        raise SQLExecutionError(f"Failed to create index: {e}") from e
    finally:
        conn.autocommit = old_autocommit


def drop_index(conn: psycopg2.extensions.connection, sql: str) -> dict:
    """Drop an index using CONCURRENTLY (requires autocommit).

    Args:
        conn: psycopg2 connection (will be set to autocommit)
        sql: DROP INDEX CONCURRENTLY statement

    Returns:
        dict with execution result
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
        raise SQLExecutionError(f"Failed to drop index: {e}") from e
    finally:
        conn.autocommit = old_autocommit

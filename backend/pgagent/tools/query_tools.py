"""Query helper tools — used by Reasoner to gather context for LLM prompts."""

from __future__ import annotations

import logging

import psycopg2

logger = logging.getLogger(__name__)


def get_table_columns(
    conn: psycopg2.extensions.connection, schema: str, table: str
) -> list[dict]:
    """Get column definitions for a table."""
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT column_name, data_type, is_nullable, column_default
               FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s
               ORDER BY ordinal_position""",
            (schema, table),
        )
        cols = []
        for row in cur.fetchall():
            cols.append({
                "column_name": row[0],
                "data_type": row[1],
                "is_nullable": row[2],
                "column_default": row[3],
            })
        cur.close()
        return cols
    except Exception as e:
        logger.warning("Failed to get columns for %s.%s: %s", schema, table, e)
        return []


def get_table_indexes(
    conn: psycopg2.extensions.connection, schema: str, table: str
) -> list[dict]:
    """Get existing indexes on a table."""
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT indexname, indexdef
               FROM pg_indexes
               WHERE schemaname = %s AND tablename = %s""",
            (schema, table),
        )
        indexes = []
        for row in cur.fetchall():
            indexes.append({"index_name": row[0], "index_def": row[1]})
        cur.close()
        return indexes
    except Exception as e:
        logger.warning("Failed to get indexes for %s.%s: %s", schema, table, e)
        return []


def get_table_queries(
    conn: psycopg2.extensions.connection, table: str, limit: int = 20
) -> list[dict]:
    """Get top queries involving a table from pg_stat_statements."""
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT query, calls, total_exec_time, mean_exec_time, rows
               FROM pg_stat_statements
               WHERE query ILIKE %s
               ORDER BY total_exec_time DESC
               LIMIT %s""",
            (f"%{table}%", limit),
        )
        queries = []
        for row in cur.fetchall():
            queries.append({
                "query": row[0],
                "calls": row[1],
                "total_exec_time": row[2],
                "mean_exec_time": row[3],
                "rows": row[4],
            })
        cur.close()
        return queries
    except Exception as e:
        logger.warning("Failed to get queries for %s: %s", table, e)
        return []

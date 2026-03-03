"""Bloat estimation tools."""

from __future__ import annotations

import logging

import psycopg2

logger = logging.getLogger(__name__)


def estimate_table_bloat(
    conn: psycopg2.extensions.connection, schema: str, table: str
) -> dict:
    """Estimate table bloat using dead tuple ratio and pg_class stats."""
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT
                 n_live_tup, n_dead_tup,
                 pg_total_relation_size(quote_ident(%s) || '.' || quote_ident(%s)) as total_size
               FROM pg_stat_user_tables
               WHERE schemaname = %s AND relname = %s""",
            (schema, table, schema, table),
        )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return {"error": f"Table {schema}.{table} not found"}
        n_live = row[0] or 0
        n_dead = row[1] or 0
        total_size = row[2] or 0
        total_tup = n_live + n_dead
        bloat_ratio = n_dead / total_tup if total_tup > 0 else 0.0
        return {
            "schema": schema,
            "table": table,
            "n_live_tup": n_live,
            "n_dead_tup": n_dead,
            "total_size_bytes": total_size,
            "bloat_ratio": round(bloat_ratio, 4),
        }
    except Exception as e:
        logger.warning("Failed to estimate bloat for %s.%s: %s", schema, table, e)
        return {"error": str(e)}

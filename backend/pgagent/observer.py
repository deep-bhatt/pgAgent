"""Observer — connects to PostgreSQL and collects snapshots from system views."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.extras

from pgagent.config import Settings
from pgagent.exceptions import (
    DeltaComputationError,
    InsufficientPrivilegesError,
    SnapshotCollectionError,
    UnsupportedVersionError,
)
from pgagent.models import (
    BgwriterStats,
    ConnectionStats,
    IndexHistory,
    IndexStats,
    LockInfo,
    QueryStats,
    Snapshot,
    SnapshotWithDeltas,
    TableDeltas,
    TableStats,
)
from pgagent.sidecar import SidecarDB

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_SQL_PG_VERSION = "SHOW server_version_num;"

_SQL_CHECK_PG_STAT_STATEMENTS = """
SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements';
"""

_SQL_CHECK_PRIVILEGES = """
SELECT 1 FROM pg_stat_user_tables LIMIT 1;
"""

_SQL_CHECK_ACTIVITY_PRIVILEGES = """
SELECT 1 FROM pg_stat_activity LIMIT 1;
"""

_SQL_TABLE_STATS = """
SELECT
    schemaname        AS schema_name,
    relname           AS table_name,
    n_live_tup,
    n_dead_tup,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_tup_hot_upd,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    vacuum_count,
    autovacuum_count,
    analyze_count,
    autoanalyze_count
FROM pg_stat_user_tables
ORDER BY schemaname, relname;
"""

_SQL_INDEX_STATS = """
SELECT
    s.schemaname       AS schema_name,
    s.relname          AS table_name,
    s.indexrelname      AS index_name,
    s.idx_scan,
    s.idx_tup_read,
    s.idx_tup_fetch,
    pg_relation_size(s.indexrelid) AS index_size_bytes,
    ix.indisunique     AS is_unique,
    ix.indisprimary    AS is_primary,
    pg_get_indexdef(s.indexrelid) AS index_def
FROM pg_stat_user_indexes s
JOIN pg_index ix ON ix.indexrelid = s.indexrelid
ORDER BY s.schemaname, s.relname, s.indexrelname;
"""

_SQL_CONNECTIONS = """
SELECT
    count(*)                                            AS total_connections,
    count(*) FILTER (WHERE state = 'active')            AS active,
    count(*) FILTER (WHERE state = 'idle')              AS idle,
    count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
    count(*) FILTER (WHERE wait_event_type IS NOT NULL AND state = 'active') AS waiting,
    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections
FROM pg_stat_activity
WHERE backend_type = 'client backend';
"""

_SQL_BGWRITER = """
SELECT
    checkpoints_timed,
    checkpoints_req,
    buffers_checkpoint,
    buffers_clean,
    maxwritten_clean,
    buffers_backend,
    buffers_alloc
FROM pg_stat_bgwriter;
"""

_SQL_LOCKS = """
SELECT
    l.pid,
    l.locktype,
    l.mode,
    l.granted,
    COALESCE(c.relname, l.locktype) AS relation,
    a.wait_event_type,
    a.wait_event,
    a.state,
    a.query,
    EXTRACT(EPOCH FROM (now() - a.state_change))::float AS wait_duration_seconds,
    pg_blocking_pids(l.pid) AS blocked_by
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
LEFT JOIN pg_class c ON c.oid = l.relation
WHERE NOT l.granted OR l.mode IN ('ExclusiveLock', 'AccessExclusiveLock',
                                   'ShareLock', 'ShareRowExclusiveLock')
ORDER BY wait_duration_seconds DESC NULLS LAST;
"""

_SQL_QUERY_STATS = """
SELECT
    queryid,
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read
FROM pg_stat_statements
WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user)
   OR TRUE
ORDER BY total_exec_time DESC
LIMIT 50;
"""

_SQL_TABLE_SIZES = """
SELECT
    n.nspname                       AS schema_name,
    c.relname                       AS table_name,
    pg_total_relation_size(c.oid)   AS table_size_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, c.relname;
"""


# ---------------------------------------------------------------------------
# Observer
# ---------------------------------------------------------------------------


class Observer:
    """Collects PostgreSQL snapshots and computes deltas."""

    def __init__(
        self,
        settings: Settings,
        conn: psycopg2.extensions.connection,
        sidecar: SidecarDB,
    ) -> None:
        self._settings = settings
        self._conn = conn
        self._sidecar = sidecar
        self._has_pg_stat_statements: bool = False
        self._pg_version: int = 0

    # ------------------------------------------------------------------
    # Startup checks
    # ------------------------------------------------------------------

    def check_connection(self) -> None:
        """Verify the connection meets all prerequisites.

        Raises:
            UnsupportedVersionError: PG version < min_pg_version (default 13).
            InsufficientPrivilegesError: Cannot read required system views.
        """
        self._check_pg_version()
        self._detect_pg_stat_statements()
        self._check_privileges()
        logger.info(
            "Connection checks passed: PG %d, pg_stat_statements=%s",
            self._pg_version,
            self._has_pg_stat_statements,
        )

    def _check_pg_version(self) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(_SQL_PG_VERSION)
                row = cur.fetchone()
                if row is None:
                    raise UnsupportedVersionError("Could not determine PostgreSQL version")
                version_num = int(row[0])
                # server_version_num is e.g. 160001 for 16.1, 130005 for 13.5
                self._pg_version = version_num // 10000
                if self._pg_version < self._settings.min_pg_version:
                    raise UnsupportedVersionError(
                        f"PostgreSQL {self._pg_version} is below minimum "
                        f"required version {self._settings.min_pg_version}"
                    )
                logger.debug("PostgreSQL version: %d (raw: %d)", self._pg_version, version_num)
        except UnsupportedVersionError:
            raise
        except Exception as exc:
            raise UnsupportedVersionError(f"Failed to check PostgreSQL version: {exc}") from exc

    def _detect_pg_stat_statements(self) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(_SQL_CHECK_PG_STAT_STATEMENTS)
                self._has_pg_stat_statements = cur.fetchone() is not None
        except Exception:
            self._has_pg_stat_statements = False
            logger.debug("pg_stat_statements not available")

    def _check_privileges(self) -> None:
        checks = [
            ("pg_stat_user_tables", _SQL_CHECK_PRIVILEGES),
            ("pg_stat_activity", _SQL_CHECK_ACTIVITY_PRIVILEGES),
        ]
        for view_name, sql in checks:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(sql)
            except Exception as exc:
                raise InsufficientPrivilegesError(
                    f"Cannot read {view_name}: {exc}"
                ) from exc

    # ------------------------------------------------------------------
    # Snapshot collection
    # ------------------------------------------------------------------

    def collect_snapshot(self) -> Snapshot:
        """Query all system views and return a Snapshot.

        Raises:
            SnapshotCollectionError: If any query fails.
        """
        timestamp = datetime.utcnow()
        try:
            tables = self._query_table_stats()
            indexes = self._query_index_stats()
            connections = self._query_connections()
            bgwriter = self._query_bgwriter()
            locks = self._query_locks()
            table_sizes = self._query_table_sizes()

            # Merge table sizes into table stats
            size_map: dict[tuple[str, str], int] = {
                (s["schema_name"], s["table_name"]): s["table_size_bytes"]
                for s in table_sizes
            }
            for tbl in tables:
                key = (tbl.schema_name, tbl.table_name)
                if key in size_map:
                    tbl.table_size_bytes = size_map[key]

            queries: list[QueryStats] = []
            if self._has_pg_stat_statements:
                queries = self._query_stat_statements()

            snapshot = Snapshot(
                timestamp=timestamp,
                tables=tables,
                indexes=indexes,
                connections=connections,
                queries=queries,
                bgwriter=bgwriter,
                locks=locks,
                pg_version=self._pg_version,
                has_pg_stat_statements=self._has_pg_stat_statements,
            )
            logger.debug(
                "Snapshot collected: %d tables, %d indexes, %d locks",
                len(tables),
                len(indexes),
                len(locks),
            )
            return snapshot

        except SnapshotCollectionError:
            raise
        except Exception as exc:
            raise SnapshotCollectionError(f"Failed to collect snapshot: {exc}") from exc

    # -- Individual query helpers --

    def _query_table_stats(self) -> list[TableStats]:
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_SQL_TABLE_STATS)
                rows = cur.fetchall()
            return [
                TableStats(
                    schema_name=r["schema_name"],
                    table_name=r["table_name"],
                    n_live_tup=r["n_live_tup"] or 0,
                    n_dead_tup=r["n_dead_tup"] or 0,
                    seq_scan=r["seq_scan"] or 0,
                    seq_tup_read=r["seq_tup_read"] or 0,
                    idx_scan=r["idx_scan"] or 0,
                    idx_tup_fetch=r["idx_tup_fetch"] or 0,
                    n_tup_ins=r["n_tup_ins"] or 0,
                    n_tup_upd=r["n_tup_upd"] or 0,
                    n_tup_del=r["n_tup_del"] or 0,
                    n_tup_hot_upd=r["n_tup_hot_upd"] or 0,
                    last_vacuum=r["last_vacuum"],
                    last_autovacuum=r["last_autovacuum"],
                    last_analyze=r["last_analyze"],
                    last_autoanalyze=r["last_autoanalyze"],
                    vacuum_count=r["vacuum_count"] or 0,
                    autovacuum_count=r["autovacuum_count"] or 0,
                    analyze_count=r["analyze_count"] or 0,
                    autoanalyze_count=r["autoanalyze_count"] or 0,
                )
                for r in rows
            ]
        except Exception as exc:
            raise SnapshotCollectionError(
                f"Failed to query pg_stat_user_tables: {exc}"
            ) from exc

    def _query_index_stats(self) -> list[IndexStats]:
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_SQL_INDEX_STATS)
                rows = cur.fetchall()
            return [
                IndexStats(
                    schema_name=r["schema_name"],
                    table_name=r["table_name"],
                    index_name=r["index_name"],
                    idx_scan=r["idx_scan"] or 0,
                    idx_tup_read=r["idx_tup_read"] or 0,
                    idx_tup_fetch=r["idx_tup_fetch"] or 0,
                    index_size_bytes=r["index_size_bytes"] or 0,
                    is_unique=r["is_unique"] or False,
                    is_primary=r["is_primary"] or False,
                    index_def=r["index_def"] or "",
                )
                for r in rows
            ]
        except Exception as exc:
            raise SnapshotCollectionError(
                f"Failed to query pg_stat_user_indexes: {exc}"
            ) from exc

    def _query_connections(self) -> ConnectionStats:
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_SQL_CONNECTIONS)
                r = cur.fetchone()
            if r is None:
                return ConnectionStats()
            return ConnectionStats(
                total_connections=r["total_connections"] or 0,
                active=r["active"] or 0,
                idle=r["idle"] or 0,
                idle_in_transaction=r["idle_in_transaction"] or 0,
                waiting=r["waiting"] or 0,
                max_connections=r["max_connections"] or 100,
            )
        except Exception as exc:
            raise SnapshotCollectionError(
                f"Failed to query pg_stat_activity: {exc}"
            ) from exc

    def _query_bgwriter(self) -> BgwriterStats:
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_SQL_BGWRITER)
                r = cur.fetchone()
            if r is None:
                return BgwriterStats()
            return BgwriterStats(
                checkpoints_timed=r["checkpoints_timed"] or 0,
                checkpoints_req=r["checkpoints_req"] or 0,
                buffers_checkpoint=r["buffers_checkpoint"] or 0,
                buffers_clean=r["buffers_clean"] or 0,
                maxwritten_clean=r["maxwritten_clean"] or 0,
                buffers_backend=r["buffers_backend"] or 0,
                buffers_alloc=r["buffers_alloc"] or 0,
            )
        except Exception as exc:
            raise SnapshotCollectionError(
                f"Failed to query pg_stat_bgwriter: {exc}"
            ) from exc

    def _query_locks(self) -> list[LockInfo]:
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_SQL_LOCKS)
                rows = cur.fetchall()
            result: list[LockInfo] = []
            for r in rows:
                blocked_by_raw = r.get("blocked_by")
                if isinstance(blocked_by_raw, list):
                    blocked_by = [int(pid) for pid in blocked_by_raw]
                elif blocked_by_raw:
                    # psycopg2 may return it as a string like {123,456}
                    cleaned = str(blocked_by_raw).strip("{}")
                    blocked_by = [int(p) for p in cleaned.split(",") if p.strip()]
                else:
                    blocked_by = []

                result.append(
                    LockInfo(
                        pid=r["pid"],
                        locktype=r["locktype"] or "",
                        mode=r["mode"] or "",
                        granted=r["granted"] if r["granted"] is not None else True,
                        relation=r["relation"],
                        wait_event_type=r["wait_event_type"],
                        wait_event=r["wait_event"],
                        state=r["state"] or "",
                        query=r["query"] or "",
                        wait_duration_seconds=r["wait_duration_seconds"],
                        blocked_by=blocked_by,
                    )
                )
            return result
        except Exception as exc:
            raise SnapshotCollectionError(
                f"Failed to query pg_locks: {exc}"
            ) from exc

    def _query_stat_statements(self) -> list[QueryStats]:
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_SQL_QUERY_STATS)
                rows = cur.fetchall()
            return [
                QueryStats(
                    queryid=r["queryid"],
                    query=r["query"] or "",
                    calls=r["calls"] or 0,
                    total_exec_time=r["total_exec_time"] or 0.0,
                    mean_exec_time=r["mean_exec_time"] or 0.0,
                    rows=r["rows"] or 0,
                    shared_blks_hit=r["shared_blks_hit"] or 0,
                    shared_blks_read=r["shared_blks_read"] or 0,
                )
                for r in rows
            ]
        except Exception as exc:
            # pg_stat_statements query failure is non-fatal; log and return empty
            logger.warning("Failed to query pg_stat_statements: %s", exc)
            return []

    def _query_table_sizes(self) -> list[dict[str, Any]]:
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_SQL_TABLE_SIZES)
                rows = cur.fetchall()
            return [
                {
                    "schema_name": r["schema_name"],
                    "table_name": r["table_name"],
                    "table_size_bytes": r["table_size_bytes"] or 0,
                }
                for r in rows
            ]
        except Exception as exc:
            logger.warning("Failed to query table sizes: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Delta computation
    # ------------------------------------------------------------------

    def compute_deltas(
        self,
        current: Snapshot,
        previous_snapshots: list[dict[str, Any]],
    ) -> SnapshotWithDeltas:
        """Compute deltas between the current snapshot and the oldest in the lookback window.

        Args:
            current: The just-collected snapshot.
            previous_snapshots: List of dicts from sidecar ``get_recent_snapshots``
                (each has ``id``, ``timestamp``, ``data`` keys).

        Returns:
            SnapshotWithDeltas with table and index deltas computed.

        Raises:
            DeltaComputationError: On unexpected failures.
        """
        try:
            table_deltas: list[TableDeltas] = []
            index_history: list[IndexHistory] = []

            if not previous_snapshots:
                # No history yet — return zeros
                return SnapshotWithDeltas(
                    snapshot=current,
                    table_deltas=table_deltas,
                    index_history=index_history,
                )

            # Use the oldest snapshot in the window for the widest delta
            prev_data = previous_snapshots[0]["data"]

            # -- Table deltas --
            prev_tables: dict[tuple[str, str], dict[str, Any]] = {}
            for t in prev_data.get("tables", []):
                key = (t.get("schema_name", "public"), t["table_name"])
                prev_tables[key] = t

            for tbl in current.tables:
                key = (tbl.schema_name, tbl.table_name)
                prev = prev_tables.get(key)
                if prev is None:
                    # New table — use current values as delta
                    table_deltas.append(
                        TableDeltas(
                            schema_name=tbl.schema_name,
                            table_name=tbl.table_name,
                            seq_scan_delta=tbl.seq_scan,
                            idx_scan_delta=tbl.idx_scan,
                            tup_ins_delta=tbl.n_tup_ins,
                            tup_upd_delta=tbl.n_tup_upd,
                            tup_del_delta=tbl.n_tup_del,
                            dead_tup_delta=tbl.n_dead_tup,
                        )
                    )
                else:
                    table_deltas.append(
                        TableDeltas(
                            schema_name=tbl.schema_name,
                            table_name=tbl.table_name,
                            seq_scan_delta=self._safe_delta(tbl.seq_scan, prev.get("seq_scan", 0)),
                            idx_scan_delta=self._safe_delta(tbl.idx_scan, prev.get("idx_scan", 0)),
                            tup_ins_delta=self._safe_delta(
                                tbl.n_tup_ins, prev.get("n_tup_ins", 0)
                            ),
                            tup_upd_delta=self._safe_delta(
                                tbl.n_tup_upd, prev.get("n_tup_upd", 0)
                            ),
                            tup_del_delta=self._safe_delta(
                                tbl.n_tup_del, prev.get("n_tup_del", 0)
                            ),
                            dead_tup_delta=self._safe_delta(
                                tbl.n_dead_tup, prev.get("n_dead_tup", 0)
                            ),
                        )
                    )

            # -- Index deltas --
            prev_indexes: dict[tuple[str, str, str], dict[str, Any]] = {}
            for idx in prev_data.get("indexes", []):
                key = (
                    idx.get("schema_name", "public"),
                    idx["table_name"],
                    idx["index_name"],
                )
                prev_indexes[key] = idx

            for idx in current.indexes:
                key = (idx.schema_name, idx.table_name, idx.index_name)
                prev = prev_indexes.get(key)
                if prev is None:
                    idx_scan_delta = idx.idx_scan
                else:
                    idx_scan_delta = self._safe_delta(idx.idx_scan, prev.get("idx_scan", 0))

                index_history.append(
                    IndexHistory(
                        schema_name=idx.schema_name,
                        table_name=idx.table_name,
                        index_name=idx.index_name,
                        idx_scan_delta=idx_scan_delta,
                    )
                )

            return SnapshotWithDeltas(
                snapshot=current,
                table_deltas=table_deltas,
                index_history=index_history,
            )

        except DeltaComputationError:
            raise
        except Exception as exc:
            raise DeltaComputationError(
                f"Failed to compute deltas: {exc}"
            ) from exc

    @staticmethod
    def _safe_delta(current: int, previous: int) -> int:
        """Compute a counter delta, treating counter resets gracefully.

        If the current value is less than the previous value, a stats reset
        has occurred (e.g. ``pg_stat_reset()``).  In that case the current
        value *is* the delta since the reset.
        """
        if current < previous:
            # Counter reset — use current as the delta
            return current
        return current - previous

    # ------------------------------------------------------------------
    # Index tracking
    # ------------------------------------------------------------------

    def update_index_tracker(self, snapshot: Snapshot) -> None:
        """Upsert every index in the snapshot into the sidecar index-scan tracker."""
        for idx in snapshot.indexes:
            try:
                self._sidecar.upsert_index_scan(
                    schema_name=idx.schema_name,
                    index_name=idx.index_name,
                    table_name=idx.table_name,
                    scan_count=idx.idx_scan,
                )
            except Exception:
                logger.warning(
                    "Failed to upsert index scan tracker for %s.%s",
                    idx.schema_name,
                    idx.index_name,
                    exc_info=True,
                )

    # ------------------------------------------------------------------
    # Full observe cycle
    # ------------------------------------------------------------------

    def observe(self) -> SnapshotWithDeltas:
        """Run one full observe cycle.

        1. Collect a snapshot from PostgreSQL.
        2. Persist the snapshot to the sidecar DB.
        3. Retrieve recent snapshots for delta computation.
        4. Compute deltas.
        5. Update the index-scan tracker.
        6. Return :class:`SnapshotWithDeltas`.
        """
        snapshot = self.collect_snapshot()

        # Save to sidecar
        snapshot_data = snapshot.model_dump(mode="json")
        snapshot_id = self._sidecar.save_snapshot(snapshot.timestamp, snapshot_data)
        logger.debug("Saved snapshot id=%d", snapshot_id)

        # Get recent snapshots for delta lookback
        lookback_hours = max(1, self._settings.delta_lookback_seconds // 3600)
        recent = self._sidecar.get_recent_snapshots(hours=lookback_hours)

        # Exclude the snapshot we just saved so we compare against prior history
        previous_snapshots = [s for s in recent if s["id"] != snapshot_id]

        # Compute deltas
        result = self.compute_deltas(snapshot, previous_snapshots)

        # Update index tracker
        self.update_index_tracker(snapshot)

        logger.info(
            "Observe cycle complete: %d table deltas, %d index history entries",
            len(result.table_deltas),
            len(result.index_history),
        )
        return result

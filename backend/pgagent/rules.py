"""Deterministic rule engine for pgAgent.

Each rule function takes (SnapshotWithDeltas, Settings) and returns
a list[Detection].  Rules are pure functions with no side-effects.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable

from pgagent.config import Settings
from pgagent.models import (
    Detection,
    DetectionType,
    Severity,
    SnapshotWithDeltas,
)

# Type alias for a rule function.
RuleFn = Callable[[SnapshotWithDeltas, Settings], list[Detection]]


# ---------------------------------------------------------------------------
# 1. check_vacuum_dead_tuples
# ---------------------------------------------------------------------------

def check_vacuum_dead_tuples(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag tables where dead-tuple ratio exceeds the threshold."""
    detections: list[Detection] = []
    for table in swd.snapshot.tables:
        total = table.n_live_tup + table.n_dead_tup
        if total <= 0:
            continue
        ratio = table.n_dead_tup / total
        if ratio > settings.dead_tuple_ratio_threshold:
            severity = (
                Severity.HIGH
                if ratio > 2 * settings.dead_tuple_ratio_threshold
                else Severity.MEDIUM
            )
            fqn = f"{table.schema_name}.{table.table_name}"
            detections.append(
                Detection(
                    detection_type=DetectionType.VACUUM_DEAD_TUPLES,
                    severity=severity,
                    target_table=fqn,
                    message=(
                        f"Table {fqn} has {table.n_dead_tup} dead tuples "
                        f"({ratio:.1%} of {total} total rows)"
                    ),
                    details={
                        "n_dead_tup": table.n_dead_tup,
                        "n_live_tup": table.n_live_tup,
                        "ratio": round(ratio, 4),
                        "threshold": settings.dead_tuple_ratio_threshold,
                    },
                )
            )
    return detections


# ---------------------------------------------------------------------------
# 2. check_vacuum_stale
# ---------------------------------------------------------------------------

def check_vacuum_stale(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag tables that have not been vacuumed within the configured window."""
    detections: list[Detection] = []
    cutoff = datetime.utcnow() - timedelta(hours=settings.vacuum_stale_hours)
    for table in swd.snapshot.tables:
        if table.n_live_tup <= 0:
            continue
        last_vac = table.last_vacuum
        last_auto = table.last_autovacuum
        # Both must be None or both older than the cutoff
        vac_stale = last_vac is None or last_vac < cutoff
        auto_stale = last_auto is None or last_auto < cutoff
        if vac_stale and auto_stale:
            fqn = f"{table.schema_name}.{table.table_name}"
            detections.append(
                Detection(
                    detection_type=DetectionType.VACUUM_STALE,
                    severity=Severity.LOW,
                    target_table=fqn,
                    message=(
                        f"Table {fqn} has not been vacuumed in over "
                        f"{settings.vacuum_stale_hours} hours"
                    ),
                    details={
                        "last_vacuum": str(last_vac),
                        "last_autovacuum": str(last_auto),
                        "stale_hours": settings.vacuum_stale_hours,
                    },
                )
            )
    return detections


# ---------------------------------------------------------------------------
# 3. check_analyze_stale
# ---------------------------------------------------------------------------

def check_analyze_stale(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag tables that have not been analyzed within the configured window."""
    detections: list[Detection] = []
    cutoff = datetime.utcnow() - timedelta(hours=settings.analyze_stale_hours)
    for table in swd.snapshot.tables:
        if table.n_live_tup <= 0:
            continue
        last_an = table.last_analyze
        last_auto = table.last_autoanalyze
        an_stale = last_an is None or last_an < cutoff
        auto_stale = last_auto is None or last_auto < cutoff
        if an_stale and auto_stale:
            fqn = f"{table.schema_name}.{table.table_name}"
            detections.append(
                Detection(
                    detection_type=DetectionType.ANALYZE_STALE,
                    severity=Severity.LOW,
                    target_table=fqn,
                    message=(
                        f"Table {fqn} has not been analyzed in over "
                        f"{settings.analyze_stale_hours} hours"
                    ),
                    details={
                        "last_analyze": str(last_an),
                        "last_autoanalyze": str(last_auto),
                        "stale_hours": settings.analyze_stale_hours,
                    },
                )
            )
    return detections


# ---------------------------------------------------------------------------
# 4. check_unused_index
# ---------------------------------------------------------------------------

_10MB = 10 * 1024 * 1024


def check_unused_index(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag indexes that are never scanned and meet the minimum size."""
    detections: list[Detection] = []

    # Build a quick lookup for index delta history
    hist_lookup: dict[str, int] = {}
    for ih in swd.index_history:
        key = f"{ih.schema_name}.{ih.table_name}.{ih.index_name}"
        hist_lookup[key] = ih.idx_scan_delta

    for idx in swd.snapshot.indexes:
        if idx.is_primary or idx.is_unique:
            continue
        if idx.idx_scan != 0:
            continue
        if idx.index_size_bytes < settings.unused_index_min_size_bytes:
            continue

        # Also check delta history — skip if scans happened in the window
        key = f"{idx.schema_name}.{idx.table_name}.{idx.index_name}"
        if hist_lookup.get(key, 0) != 0:
            continue

        severity = (
            Severity.MEDIUM
            if idx.index_size_bytes > _10MB
            else Severity.LOW
        )
        fqn_idx = f"{idx.schema_name}.{idx.index_name}"
        fqn_tbl = f"{idx.schema_name}.{idx.table_name}"
        detections.append(
            Detection(
                detection_type=DetectionType.UNUSED_INDEX,
                severity=severity,
                target_table=fqn_tbl,
                target_index=fqn_idx,
                message=(
                    f"Index {fqn_idx} on {fqn_tbl} has 0 scans and "
                    f"occupies {idx.index_size_bytes:,} bytes"
                ),
                details={
                    "index_name": idx.index_name,
                    "table_name": idx.table_name,
                    "index_size_bytes": idx.index_size_bytes,
                    "idx_scan": idx.idx_scan,
                    "index_def": idx.index_def,
                },
            )
        )
    return detections


# ---------------------------------------------------------------------------
# 5. check_seq_scan_heavy
# ---------------------------------------------------------------------------

def check_seq_scan_heavy(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag tables where sequential scans dominate index scans."""
    detections: list[Detection] = []
    for td in swd.table_deltas:
        total_scans = td.seq_scan_delta + td.idx_scan_delta
        if total_scans <= 0:
            continue
        if td.seq_scan_delta < settings.seq_scan_min_total:
            continue
        ratio = td.seq_scan_delta / total_scans
        if ratio > settings.seq_scan_ratio_threshold:
            fqn = f"{td.schema_name}.{td.table_name}"
            detections.append(
                Detection(
                    detection_type=DetectionType.SEQ_SCAN_HEAVY,
                    severity=Severity.MEDIUM,
                    target_table=fqn,
                    message=(
                        f"Table {fqn} has {td.seq_scan_delta} seq scans vs "
                        f"{td.idx_scan_delta} idx scans ({ratio:.1%} seq)"
                    ),
                    details={
                        "seq_scan_delta": td.seq_scan_delta,
                        "idx_scan_delta": td.idx_scan_delta,
                        "ratio": round(ratio, 4),
                        "threshold": settings.seq_scan_ratio_threshold,
                    },
                    llm_reasoning_needed=True,
                )
            )
    return detections


# ---------------------------------------------------------------------------
# 6. check_idle_in_transaction
# ---------------------------------------------------------------------------

def check_idle_in_transaction(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag connections that are idle in transaction longer than allowed."""
    detections: list[Detection] = []
    for lock in swd.snapshot.locks:
        if lock.state != "idle in transaction":
            continue
        duration = lock.wait_duration_seconds
        if duration is None or duration <= settings.idle_transaction_seconds:
            continue
        severity = (
            Severity.HIGH
            if duration > 2 * settings.idle_transaction_seconds
            else Severity.MEDIUM
        )
        detections.append(
            Detection(
                detection_type=DetectionType.IDLE_IN_TRANSACTION,
                severity=severity,
                target_pid=lock.pid,
                message=(
                    f"PID {lock.pid} idle in transaction for "
                    f"{duration:.0f}s (threshold {settings.idle_transaction_seconds}s)"
                ),
                details={
                    "pid": lock.pid,
                    "wait_duration_seconds": duration,
                    "state": lock.state,
                    "query": lock.query,
                },
            )
        )
    return detections


# ---------------------------------------------------------------------------
# 7. check_lock_contention
# ---------------------------------------------------------------------------

def check_lock_contention(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag locks that are not granted and have been waiting too long."""
    detections: list[Detection] = []
    for lock in swd.snapshot.locks:
        if lock.granted:
            continue
        duration = lock.wait_duration_seconds
        if duration is None or duration <= settings.lock_wait_seconds:
            continue
        detections.append(
            Detection(
                detection_type=DetectionType.LOCK_CONTENTION,
                severity=Severity.HIGH,
                target_pid=lock.pid,
                message=(
                    f"PID {lock.pid} waiting for {lock.mode} lock for "
                    f"{duration:.0f}s (threshold {settings.lock_wait_seconds}s)"
                ),
                details={
                    "pid": lock.pid,
                    "locktype": lock.locktype,
                    "mode": lock.mode,
                    "wait_duration_seconds": duration,
                    "blocked_by": lock.blocked_by,
                    "relation": lock.relation,
                },
            )
        )
    return detections


# ---------------------------------------------------------------------------
# 8. check_connection_saturation
# ---------------------------------------------------------------------------

def check_connection_saturation(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag when the ratio of active connections to max_connections is too high."""
    detections: list[Detection] = []
    conn = swd.snapshot.connections
    if conn.max_connections <= 0:
        return detections
    ratio = conn.total_connections / conn.max_connections
    if ratio > settings.connection_saturation_ratio:
        severity = Severity.CRITICAL if ratio > 0.9 else Severity.HIGH
        detections.append(
            Detection(
                detection_type=DetectionType.CONNECTION_SATURATION,
                severity=severity,
                message=(
                    f"Connection pool at {ratio:.1%} capacity "
                    f"({conn.total_connections}/{conn.max_connections})"
                ),
                details={
                    "total_connections": conn.total_connections,
                    "max_connections": conn.max_connections,
                    "ratio": round(ratio, 4),
                    "threshold": settings.connection_saturation_ratio,
                },
            )
        )
    return detections


# ---------------------------------------------------------------------------
# 9. check_table_bloat
# ---------------------------------------------------------------------------

def check_table_bloat(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Estimate table bloat using dead-tuple ratio as proxy."""
    detections: list[Detection] = []
    for table in swd.snapshot.tables:
        total = table.n_live_tup + table.n_dead_tup
        if total <= 0:
            continue
        ratio = table.n_dead_tup / total
        if ratio > settings.table_bloat_ratio_threshold:
            fqn = f"{table.schema_name}.{table.table_name}"
            detections.append(
                Detection(
                    detection_type=DetectionType.TABLE_BLOAT,
                    severity=Severity.HIGH,
                    target_table=fqn,
                    message=(
                        f"Table {fqn} estimated bloat {ratio:.1%} "
                        f"(threshold {settings.table_bloat_ratio_threshold:.1%})"
                    ),
                    details={
                        "n_dead_tup": table.n_dead_tup,
                        "n_live_tup": table.n_live_tup,
                        "ratio": round(ratio, 4),
                        "threshold": settings.table_bloat_ratio_threshold,
                    },
                )
            )
    return detections


# ---------------------------------------------------------------------------
# 10. check_high_backend_writes
# ---------------------------------------------------------------------------

def check_high_backend_writes(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag when backends are writing too many buffers directly."""
    detections: list[Detection] = []
    bg = swd.snapshot.bgwriter
    alloc = max(bg.buffers_alloc, 1)
    ratio = bg.buffers_backend / alloc
    if ratio > settings.bgwriter_maxwritten_ratio:
        detections.append(
            Detection(
                detection_type=DetectionType.HIGH_BACKEND_WRITES,
                severity=Severity.MEDIUM,
                message=(
                    f"Backend buffer writes ratio {ratio:.1%} "
                    f"({bg.buffers_backend}/{alloc}) exceeds threshold "
                    f"{settings.bgwriter_maxwritten_ratio:.1%}"
                ),
                details={
                    "buffers_backend": bg.buffers_backend,
                    "buffers_alloc": bg.buffers_alloc,
                    "ratio": round(ratio, 4),
                    "threshold": settings.bgwriter_maxwritten_ratio,
                },
            )
        )
    return detections


# ---------------------------------------------------------------------------
# 11. check_forced_checkpoints
# ---------------------------------------------------------------------------

def check_forced_checkpoints(
    swd: SnapshotWithDeltas,
    settings: Settings,
) -> list[Detection]:
    """Flag when the ratio of requested (forced) checkpoints is too high."""
    detections: list[Detection] = []
    bg = swd.snapshot.bgwriter
    total_cp = bg.checkpoints_req + bg.checkpoints_timed
    if total_cp <= 0:
        return detections
    ratio = bg.checkpoints_req / total_cp
    if ratio > settings.checkpoint_warning_ratio:
        detections.append(
            Detection(
                detection_type=DetectionType.FORCED_CHECKPOINTS,
                severity=Severity.MEDIUM,
                message=(
                    f"Forced checkpoint ratio {ratio:.1%} "
                    f"({bg.checkpoints_req} req / {total_cp} total) "
                    f"exceeds threshold {settings.checkpoint_warning_ratio:.1%}"
                ),
                details={
                    "checkpoints_req": bg.checkpoints_req,
                    "checkpoints_timed": bg.checkpoints_timed,
                    "ratio": round(ratio, 4),
                    "threshold": settings.checkpoint_warning_ratio,
                },
            )
        )
    return detections


# ---------------------------------------------------------------------------
# ALL_RULES registry
# ---------------------------------------------------------------------------

ALL_RULES: list[RuleFn] = [
    check_vacuum_dead_tuples,
    check_vacuum_stale,
    check_analyze_stale,
    check_unused_index,
    check_seq_scan_heavy,
    check_idle_in_transaction,
    check_lock_contention,
    check_connection_saturation,
    check_table_bloat,
    check_high_backend_writes,
    check_forced_checkpoints,
]

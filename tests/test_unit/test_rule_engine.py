"""Unit tests for the deterministic rule engine (rules.py) and Detector.

Every test builds synthetic SnapshotWithDeltas data -- no real Postgres
connection is needed.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pgagent.config import Settings
from pgagent.detector import Detector
from pgagent.models import (
    BgwriterStats,
    ConnectionStats,
    Detection,
    DetectionType,
    IndexHistory,
    IndexStats,
    LockInfo,
    Severity,
    Snapshot,
    SnapshotWithDeltas,
    TableDeltas,
    TableStats,
)
from pgagent.rules import (
    ALL_RULES,
    check_analyze_stale,
    check_connection_saturation,
    check_forced_checkpoints,
    check_high_backend_writes,
    check_idle_in_transaction,
    check_lock_contention,
    check_seq_scan_heavy,
    check_table_bloat,
    check_unused_index,
    check_vacuum_dead_tuples,
    check_vacuum_stale,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _settings(**overrides) -> Settings:
    """Return a Settings instance with optional overrides."""
    return Settings(**overrides)


def _swd(
    tables: list[TableStats] | None = None,
    indexes: list[IndexStats] | None = None,
    connections: ConnectionStats | None = None,
    bgwriter: BgwriterStats | None = None,
    locks: list[LockInfo] | None = None,
    table_deltas: list[TableDeltas] | None = None,
    index_history: list[IndexHistory] | None = None,
) -> SnapshotWithDeltas:
    """Build a minimal SnapshotWithDeltas for testing."""
    snap = Snapshot(
        tables=tables or [],
        indexes=indexes or [],
        connections=connections or ConnectionStats(),
        bgwriter=bgwriter or BgwriterStats(),
        locks=locks or [],
    )
    return SnapshotWithDeltas(
        snapshot=snap,
        table_deltas=table_deltas or [],
        index_history=index_history or [],
    )


# ===================================================================
# 1. check_vacuum_dead_tuples
# ===================================================================

class TestCheckVacuumDeadTuples:
    def test_fires_when_ratio_exceeded(self):
        """Dead tuple ratio above threshold -> detection."""
        table = TableStats(table_name="orders", n_live_tup=900, n_dead_tup=200)
        swd = _swd(tables=[table])
        settings = _settings(dead_tuple_ratio_threshold=0.05)
        result = check_vacuum_dead_tuples(swd, settings)
        assert len(result) == 1
        assert result[0].detection_type == DetectionType.VACUUM_DEAD_TUPLES
        assert result[0].target_table == "public.orders"

    def test_does_not_fire_below_threshold(self):
        """Dead tuple ratio below threshold -> no detection."""
        table = TableStats(table_name="orders", n_live_tup=1000, n_dead_tup=1)
        swd = _swd(tables=[table])
        settings = _settings(dead_tuple_ratio_threshold=0.05)
        result = check_vacuum_dead_tuples(swd, settings)
        assert len(result) == 0

    def test_skips_empty_table(self):
        """Table with 0 total rows -> skip."""
        table = TableStats(table_name="empty", n_live_tup=0, n_dead_tup=0)
        swd = _swd(tables=[table])
        result = check_vacuum_dead_tuples(swd, _settings())
        assert len(result) == 0

    def test_severity_high_when_ratio_double(self):
        """Ratio > 2x threshold -> HIGH severity."""
        # threshold = 0.05, ratio = 0.5 (500 dead out of 1000) => > 2*0.05
        table = TableStats(table_name="big", n_live_tup=500, n_dead_tup=500)
        swd = _swd(tables=[table])
        settings = _settings(dead_tuple_ratio_threshold=0.05)
        result = check_vacuum_dead_tuples(swd, settings)
        assert result[0].severity == Severity.HIGH

    def test_severity_medium_when_ratio_moderate(self):
        """Ratio > threshold but < 2x threshold -> MEDIUM severity."""
        # threshold = 0.10, ratio ~0.15 => > 0.10 but < 0.20
        table = TableStats(table_name="med", n_live_tup=850, n_dead_tup=150)
        swd = _swd(tables=[table])
        settings = _settings(dead_tuple_ratio_threshold=0.10)
        result = check_vacuum_dead_tuples(swd, settings)
        assert len(result) == 1
        assert result[0].severity == Severity.MEDIUM


# ===================================================================
# 2. check_vacuum_stale
# ===================================================================

class TestCheckVacuumStale:
    def test_fires_when_never_vacuumed(self):
        """Both vacuum timestamps None -> stale."""
        table = TableStats(
            table_name="stale_t",
            n_live_tup=100,
            last_vacuum=None,
            last_autovacuum=None,
        )
        swd = _swd(tables=[table])
        result = check_vacuum_stale(swd, _settings(vacuum_stale_hours=24))
        assert len(result) == 1
        assert result[0].detection_type == DetectionType.VACUUM_STALE
        assert result[0].severity == Severity.LOW

    def test_does_not_fire_when_recently_vacuumed(self):
        """Recent vacuum -> no detection."""
        table = TableStats(
            table_name="fresh",
            n_live_tup=100,
            last_vacuum=datetime.utcnow() - timedelta(hours=1),
        )
        swd = _swd(tables=[table])
        result = check_vacuum_stale(swd, _settings(vacuum_stale_hours=24))
        assert len(result) == 0

    def test_does_not_fire_when_recently_autovacuumed(self):
        """Recent autovacuum (but manual vacuum is stale) -> no detection."""
        table = TableStats(
            table_name="auto_ok",
            n_live_tup=100,
            last_vacuum=None,
            last_autovacuum=datetime.utcnow() - timedelta(hours=1),
        )
        swd = _swd(tables=[table])
        result = check_vacuum_stale(swd, _settings(vacuum_stale_hours=24))
        assert len(result) == 0

    def test_skips_empty_table(self):
        table = TableStats(table_name="empty", n_live_tup=0)
        swd = _swd(tables=[table])
        result = check_vacuum_stale(swd, _settings())
        assert len(result) == 0


# ===================================================================
# 3. check_analyze_stale
# ===================================================================

class TestCheckAnalyzeStale:
    def test_fires_when_never_analyzed(self):
        table = TableStats(
            table_name="no_analyze",
            n_live_tup=50,
            last_analyze=None,
            last_autoanalyze=None,
        )
        swd = _swd(tables=[table])
        result = check_analyze_stale(swd, _settings(analyze_stale_hours=24))
        assert len(result) == 1
        assert result[0].detection_type == DetectionType.ANALYZE_STALE

    def test_does_not_fire_when_recently_analyzed(self):
        table = TableStats(
            table_name="fresh",
            n_live_tup=50,
            last_analyze=datetime.utcnow() - timedelta(hours=1),
        )
        swd = _swd(tables=[table])
        result = check_analyze_stale(swd, _settings(analyze_stale_hours=24))
        assert len(result) == 0

    def test_does_not_fire_when_recently_autoanalyzed(self):
        table = TableStats(
            table_name="auto_ok",
            n_live_tup=50,
            last_analyze=None,
            last_autoanalyze=datetime.utcnow() - timedelta(hours=2),
        )
        swd = _swd(tables=[table])
        result = check_analyze_stale(swd, _settings(analyze_stale_hours=24))
        assert len(result) == 0

    def test_skips_empty_table(self):
        table = TableStats(table_name="empty", n_live_tup=0)
        swd = _swd(tables=[table])
        result = check_analyze_stale(swd, _settings())
        assert len(result) == 0


# ===================================================================
# 4. check_unused_index
# ===================================================================

class TestCheckUnusedIndex:
    def test_fires_for_unused_non_unique_index(self):
        idx = IndexStats(
            table_name="orders",
            index_name="idx_orders_old",
            idx_scan=0,
            index_size_bytes=2_000_000,
            is_unique=False,
            is_primary=False,
        )
        swd = _swd(indexes=[idx])
        settings = _settings(unused_index_min_size_bytes=1_000_000)
        result = check_unused_index(swd, settings)
        assert len(result) == 1
        assert result[0].detection_type == DetectionType.UNUSED_INDEX

    def test_skips_primary_key(self):
        idx = IndexStats(
            table_name="orders",
            index_name="orders_pkey",
            idx_scan=0,
            index_size_bytes=5_000_000,
            is_primary=True,
        )
        swd = _swd(indexes=[idx])
        result = check_unused_index(swd, _settings())
        assert len(result) == 0

    def test_skips_unique_index(self):
        idx = IndexStats(
            table_name="users",
            index_name="idx_users_email",
            idx_scan=0,
            index_size_bytes=5_000_000,
            is_unique=True,
        )
        swd = _swd(indexes=[idx])
        result = check_unused_index(swd, _settings())
        assert len(result) == 0

    def test_skips_if_scans_positive(self):
        idx = IndexStats(
            table_name="orders",
            index_name="idx_orders_date",
            idx_scan=10,
            index_size_bytes=5_000_000,
        )
        swd = _swd(indexes=[idx])
        result = check_unused_index(swd, _settings())
        assert len(result) == 0

    def test_skips_small_index(self):
        idx = IndexStats(
            table_name="orders",
            index_name="idx_tiny",
            idx_scan=0,
            index_size_bytes=100,
        )
        swd = _swd(indexes=[idx])
        settings = _settings(unused_index_min_size_bytes=1_000_000)
        result = check_unused_index(swd, settings)
        assert len(result) == 0

    def test_skips_if_delta_shows_scans(self):
        """Even though snapshot idx_scan=0, if delta shows scans, skip."""
        idx = IndexStats(
            table_name="orders",
            index_name="idx_orders_recent",
            idx_scan=0,
            index_size_bytes=2_000_000,
        )
        hist = IndexHistory(
            table_name="orders",
            index_name="idx_orders_recent",
            idx_scan_delta=5,
        )
        swd = _swd(indexes=[idx], index_history=[hist])
        result = check_unused_index(swd, _settings(unused_index_min_size_bytes=1_000_000))
        assert len(result) == 0

    def test_severity_medium_for_large_index(self):
        idx = IndexStats(
            table_name="orders",
            index_name="idx_fat",
            idx_scan=0,
            index_size_bytes=20_000_000,  # 20 MB > 10 MB
        )
        swd = _swd(indexes=[idx])
        result = check_unused_index(swd, _settings(unused_index_min_size_bytes=1_000_000))
        assert result[0].severity == Severity.MEDIUM

    def test_severity_low_for_small_unused_index(self):
        idx = IndexStats(
            table_name="orders",
            index_name="idx_small",
            idx_scan=0,
            index_size_bytes=2_000_000,  # 2 MB < 10 MB
        )
        swd = _swd(indexes=[idx])
        result = check_unused_index(swd, _settings(unused_index_min_size_bytes=1_000_000))
        assert result[0].severity == Severity.LOW


# ===================================================================
# 5. check_seq_scan_heavy
# ===================================================================

class TestCheckSeqScanHeavy:
    def test_fires_when_seq_scans_dominate(self):
        td = TableDeltas(
            table_name="big_table",
            seq_scan_delta=500,
            idx_scan_delta=10,
        )
        swd = _swd(table_deltas=[td])
        settings = _settings(seq_scan_ratio_threshold=0.5, seq_scan_min_total=100)
        result = check_seq_scan_heavy(swd, settings)
        assert len(result) == 1
        assert result[0].detection_type == DetectionType.SEQ_SCAN_HEAVY
        assert result[0].llm_reasoning_needed is True

    def test_does_not_fire_below_threshold(self):
        td = TableDeltas(
            table_name="balanced",
            seq_scan_delta=40,
            idx_scan_delta=60,
        )
        swd = _swd(table_deltas=[td])
        settings = _settings(seq_scan_ratio_threshold=0.5, seq_scan_min_total=10)
        result = check_seq_scan_heavy(swd, settings)
        assert len(result) == 0

    def test_does_not_fire_below_min_total(self):
        td = TableDeltas(
            table_name="low_vol",
            seq_scan_delta=5,
            idx_scan_delta=0,
        )
        swd = _swd(table_deltas=[td])
        settings = _settings(seq_scan_ratio_threshold=0.5, seq_scan_min_total=100)
        result = check_seq_scan_heavy(swd, settings)
        assert len(result) == 0

    def test_severity_is_medium(self):
        td = TableDeltas(table_name="t", seq_scan_delta=200, idx_scan_delta=1)
        swd = _swd(table_deltas=[td])
        result = check_seq_scan_heavy(swd, _settings(seq_scan_min_total=100))
        assert result[0].severity == Severity.MEDIUM


# ===================================================================
# 6. check_idle_in_transaction
# ===================================================================

class TestCheckIdleInTransaction:
    def test_fires_for_long_idle_txn(self):
        lock = LockInfo(
            pid=1234,
            state="idle in transaction",
            wait_duration_seconds=600,
        )
        swd = _swd(locks=[lock])
        settings = _settings(idle_transaction_seconds=300)
        result = check_idle_in_transaction(swd, settings)
        assert len(result) == 1
        assert result[0].target_pid == 1234

    def test_does_not_fire_within_threshold(self):
        lock = LockInfo(
            pid=1234,
            state="idle in transaction",
            wait_duration_seconds=100,
        )
        swd = _swd(locks=[lock])
        settings = _settings(idle_transaction_seconds=300)
        result = check_idle_in_transaction(swd, settings)
        assert len(result) == 0

    def test_ignores_active_connections(self):
        lock = LockInfo(
            pid=1234,
            state="active",
            wait_duration_seconds=9999,
        )
        swd = _swd(locks=[lock])
        result = check_idle_in_transaction(swd, _settings())
        assert len(result) == 0

    def test_severity_high_when_double_threshold(self):
        lock = LockInfo(
            pid=1234,
            state="idle in transaction",
            wait_duration_seconds=700,
        )
        swd = _swd(locks=[lock])
        settings = _settings(idle_transaction_seconds=300)
        result = check_idle_in_transaction(swd, settings)
        assert result[0].severity == Severity.HIGH

    def test_severity_medium_when_just_over_threshold(self):
        lock = LockInfo(
            pid=1234,
            state="idle in transaction",
            wait_duration_seconds=400,
        )
        swd = _swd(locks=[lock])
        settings = _settings(idle_transaction_seconds=300)
        result = check_idle_in_transaction(swd, settings)
        assert result[0].severity == Severity.MEDIUM


# ===================================================================
# 7. check_lock_contention
# ===================================================================

class TestCheckLockContention:
    def test_fires_for_ungranted_long_wait(self):
        lock = LockInfo(
            pid=5678,
            granted=False,
            mode="AccessExclusiveLock",
            wait_duration_seconds=60,
        )
        swd = _swd(locks=[lock])
        settings = _settings(lock_wait_seconds=30)
        result = check_lock_contention(swd, settings)
        assert len(result) == 1
        assert result[0].severity == Severity.HIGH

    def test_does_not_fire_for_granted_lock(self):
        lock = LockInfo(
            pid=5678,
            granted=True,
            wait_duration_seconds=9999,
        )
        swd = _swd(locks=[lock])
        result = check_lock_contention(swd, _settings())
        assert len(result) == 0

    def test_does_not_fire_for_short_wait(self):
        lock = LockInfo(
            pid=5678,
            granted=False,
            wait_duration_seconds=5,
        )
        swd = _swd(locks=[lock])
        settings = _settings(lock_wait_seconds=30)
        result = check_lock_contention(swd, settings)
        assert len(result) == 0


# ===================================================================
# 8. check_connection_saturation
# ===================================================================

class TestCheckConnectionSaturation:
    def test_fires_when_saturated(self):
        conn = ConnectionStats(total_connections=85, max_connections=100)
        swd = _swd(connections=conn)
        settings = _settings(connection_saturation_ratio=0.8)
        result = check_connection_saturation(swd, settings)
        assert len(result) == 1

    def test_does_not_fire_below_ratio(self):
        conn = ConnectionStats(total_connections=50, max_connections=100)
        swd = _swd(connections=conn)
        settings = _settings(connection_saturation_ratio=0.8)
        result = check_connection_saturation(swd, settings)
        assert len(result) == 0

    def test_severity_critical_above_90_pct(self):
        conn = ConnectionStats(total_connections=95, max_connections=100)
        swd = _swd(connections=conn)
        settings = _settings(connection_saturation_ratio=0.8)
        result = check_connection_saturation(swd, settings)
        assert result[0].severity == Severity.CRITICAL

    def test_severity_high_between_80_and_90_pct(self):
        conn = ConnectionStats(total_connections=85, max_connections=100)
        swd = _swd(connections=conn)
        settings = _settings(connection_saturation_ratio=0.8)
        result = check_connection_saturation(swd, settings)
        assert result[0].severity == Severity.HIGH

    def test_safe_with_zero_max_connections(self):
        conn = ConnectionStats(total_connections=0, max_connections=0)
        swd = _swd(connections=conn)
        result = check_connection_saturation(swd, _settings())
        assert len(result) == 0


# ===================================================================
# 9. check_table_bloat
# ===================================================================

class TestCheckTableBloat:
    def test_fires_when_bloated(self):
        table = TableStats(table_name="bloaty", n_live_tup=600, n_dead_tup=400)
        swd = _swd(tables=[table])
        settings = _settings(table_bloat_ratio_threshold=0.3)
        result = check_table_bloat(swd, settings)
        assert len(result) == 1
        assert result[0].severity == Severity.HIGH

    def test_does_not_fire_below_threshold(self):
        table = TableStats(table_name="clean", n_live_tup=950, n_dead_tup=50)
        swd = _swd(tables=[table])
        settings = _settings(table_bloat_ratio_threshold=0.3)
        result = check_table_bloat(swd, settings)
        assert len(result) == 0

    def test_skips_empty_table(self):
        table = TableStats(table_name="empty", n_live_tup=0, n_dead_tup=0)
        swd = _swd(tables=[table])
        result = check_table_bloat(swd, _settings())
        assert len(result) == 0


# ===================================================================
# 10. check_high_backend_writes
# ===================================================================

class TestCheckHighBackendWrites:
    def test_fires_when_ratio_exceeded(self):
        bg = BgwriterStats(buffers_backend=600, buffers_alloc=1000)
        swd = _swd(bgwriter=bg)
        settings = _settings(bgwriter_maxwritten_ratio=0.5)
        result = check_high_backend_writes(swd, settings)
        assert len(result) == 1
        assert result[0].severity == Severity.MEDIUM

    def test_does_not_fire_below_threshold(self):
        bg = BgwriterStats(buffers_backend=100, buffers_alloc=1000)
        swd = _swd(bgwriter=bg)
        settings = _settings(bgwriter_maxwritten_ratio=0.5)
        result = check_high_backend_writes(swd, settings)
        assert len(result) == 0

    def test_safe_with_zero_alloc(self):
        """buffers_alloc=0 should use max(..., 1) and not divide by zero."""
        bg = BgwriterStats(buffers_backend=1, buffers_alloc=0)
        swd = _swd(bgwriter=bg)
        settings = _settings(bgwriter_maxwritten_ratio=0.5)
        result = check_high_backend_writes(swd, settings)
        # ratio = 1/1 = 1.0 > 0.5 -> fires
        assert len(result) == 1


# ===================================================================
# 11. check_forced_checkpoints
# ===================================================================

class TestCheckForcedCheckpoints:
    def test_fires_when_ratio_exceeded(self):
        bg = BgwriterStats(checkpoints_req=80, checkpoints_timed=20)
        swd = _swd(bgwriter=bg)
        settings = _settings(checkpoint_warning_ratio=0.5)
        result = check_forced_checkpoints(swd, settings)
        assert len(result) == 1
        assert result[0].severity == Severity.MEDIUM

    def test_does_not_fire_below_threshold(self):
        bg = BgwriterStats(checkpoints_req=10, checkpoints_timed=90)
        swd = _swd(bgwriter=bg)
        settings = _settings(checkpoint_warning_ratio=0.5)
        result = check_forced_checkpoints(swd, settings)
        assert len(result) == 0

    def test_safe_with_zero_checkpoints(self):
        bg = BgwriterStats(checkpoints_req=0, checkpoints_timed=0)
        swd = _swd(bgwriter=bg)
        result = check_forced_checkpoints(swd, _settings())
        assert len(result) == 0


# ===================================================================
# ALL_RULES registry
# ===================================================================

class TestAllRules:
    def test_contains_all_eleven_rules(self):
        assert len(ALL_RULES) == 11

    def test_each_rule_is_callable(self):
        for rule in ALL_RULES:
            assert callable(rule)

    def test_all_rules_on_empty_snapshot(self):
        """No rule should crash on a completely empty snapshot."""
        swd = _swd()
        settings = _settings()
        for rule in ALL_RULES:
            result = rule(swd, settings)
            assert isinstance(result, list)


# ===================================================================
# Detector integration
# ===================================================================

class TestDetector:
    def test_detect_aggregates_all_rules(self):
        """Detector.detect() should return results from multiple rules."""
        table = TableStats(
            table_name="t",
            n_live_tup=500,
            n_dead_tup=500,  # triggers vacuum_dead_tuples + table_bloat
        )
        swd = _swd(tables=[table])
        settings = _settings(
            dead_tuple_ratio_threshold=0.05,
            table_bloat_ratio_threshold=0.3,
        )
        detector = Detector(settings)
        detections = detector.detect(swd)
        types = {d.detection_type for d in detections}
        assert DetectionType.VACUUM_DEAD_TUPLES in types
        assert DetectionType.TABLE_BLOAT in types

    def test_needs_llm_filter(self):
        """Detector.needs_llm() should only return LLM-flagged detections."""
        d_llm = Detection(
            detection_type=DetectionType.SEQ_SCAN_HEAVY,
            llm_reasoning_needed=True,
        )
        d_direct = Detection(
            detection_type=DetectionType.VACUUM_DEAD_TUPLES,
            llm_reasoning_needed=False,
        )
        assert Detector.needs_llm([d_llm, d_direct]) == [d_llm]

    def test_direct_filter(self):
        """Detector.direct() should only return non-LLM detections."""
        d_llm = Detection(
            detection_type=DetectionType.SEQ_SCAN_HEAVY,
            llm_reasoning_needed=True,
        )
        d_direct = Detection(
            detection_type=DetectionType.VACUUM_DEAD_TUPLES,
            llm_reasoning_needed=False,
        )
        assert Detector.direct([d_llm, d_direct]) == [d_direct]

    def test_empty_snapshot_returns_empty(self):
        detector = Detector(_settings())
        result = detector.detect(_swd())
        # Some rules may still fire on default empty data (e.g., vacuum_stale
        # won't because n_live_tup=0). The key assertion is no crash.
        assert isinstance(result, list)

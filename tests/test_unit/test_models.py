"""Tests for Pydantic models and enums."""

from datetime import datetime

from pgagent.models import (
    Action,
    ActionOutcome,
    ActionType,
    BgwriterStats,
    ConnectionStats,
    Detection,
    DetectionType,
    IndexStats,
    LLMIndexRecommendation,
    LLMPrioritizationResult,
    LockInfo,
    QueryStats,
    RiskLevel,
    Severity,
    Snapshot,
    SnapshotWithDeltas,
    Suggestion,
    SuggestionStatus,
    TableDeltas,
    TableStats,
)


class TestEnums:
    def test_detection_type_values(self):
        assert DetectionType.VACUUM_DEAD_TUPLES == "vacuum_dead_tuples"
        assert DetectionType.SEQ_SCAN_HEAVY == "seq_scan_heavy"
        assert len(DetectionType) == 11

    def test_severity_values(self):
        assert Severity.LOW == "low"
        assert Severity.CRITICAL == "critical"

    def test_suggestion_status_values(self):
        assert SuggestionStatus.PENDING == "pending"
        assert SuggestionStatus.EVALUATED == "evaluated"
        assert len(SuggestionStatus) == 6

    def test_action_type_values(self):
        assert ActionType.CREATE_INDEX == "create_index"
        assert len(ActionType) == 5

    def test_action_outcome_values(self):
        assert ActionOutcome.ROLLED_BACK == "rolled_back"
        assert len(ActionOutcome) == 7


class TestTableStats:
    def test_defaults(self):
        ts = TableStats(table_name="users")
        assert ts.schema_name == "public"
        assert ts.n_live_tup == 0
        assert ts.last_vacuum is None

    def test_full_construction(self):
        ts = TableStats(
            table_name="orders",
            n_live_tup=1000,
            n_dead_tup=50,
            seq_scan=100,
            idx_scan=500,
        )
        assert ts.n_dead_tup == 50


class TestIndexStats:
    def test_defaults(self):
        idx = IndexStats(table_name="users", index_name="users_pkey")
        assert idx.idx_scan == 0
        assert not idx.is_unique

    def test_primary_key(self):
        idx = IndexStats(
            table_name="users", index_name="users_pkey", is_primary=True, is_unique=True
        )
        assert idx.is_primary


class TestSnapshot:
    def test_empty_snapshot(self):
        snap = Snapshot()
        assert snap.tables == []
        assert snap.indexes == []
        assert snap.pg_version == 16

    def test_snapshot_with_data(self):
        snap = Snapshot(
            tables=[TableStats(table_name="t1")],
            indexes=[IndexStats(table_name="t1", index_name="i1")],
            connections=ConnectionStats(total_connections=10),
        )
        assert len(snap.tables) == 1
        assert snap.connections.total_connections == 10


class TestSnapshotWithDeltas:
    def test_construction(self):
        swd = SnapshotWithDeltas(
            snapshot=Snapshot(),
            table_deltas=[TableDeltas(table_name="t1", seq_scan_delta=10)],
        )
        assert swd.table_deltas[0].seq_scan_delta == 10


class TestDetection:
    def test_defaults(self):
        d = Detection(detection_type=DetectionType.VACUUM_DEAD_TUPLES)
        assert d.severity == Severity.MEDIUM
        assert not d.llm_reasoning_needed

    def test_seq_scan_needs_llm(self):
        d = Detection(
            detection_type=DetectionType.SEQ_SCAN_HEAVY,
            llm_reasoning_needed=True,
            target_table="orders",
        )
        assert d.llm_reasoning_needed


class TestSuggestion:
    def test_defaults(self):
        s = Suggestion(action_type=ActionType.VACUUM)
        assert s.status == SuggestionStatus.PENDING
        assert s.reversible

    def test_serialization_roundtrip(self):
        s = Suggestion(
            action_type=ActionType.CREATE_INDEX,
            target_table="orders",
            sql="CREATE INDEX CONCURRENTLY ...",
            risk_level=RiskLevel.MEDIUM,
        )
        data = s.model_dump()
        s2 = Suggestion(**data)
        assert s2.sql == s.sql
        assert s2.risk_level == RiskLevel.MEDIUM


class TestAction:
    def test_defaults(self):
        a = Action(suggestion_id=1, action_type=ActionType.VACUUM)
        assert a.outcome == ActionOutcome.PENDING_EVALUATION
        assert not a.rolled_back

    def test_serialization_roundtrip(self):
        a = Action(
            suggestion_id=1,
            action_type=ActionType.CREATE_INDEX,
            sql_executed="CREATE INDEX CONCURRENTLY idx ON t(c)",
            pre_snapshot={"seq_scan": 100},
        )
        data = a.model_dump()
        a2 = Action(**data)
        assert a2.pre_snapshot == {"seq_scan": 100}


class TestLLMModels:
    def test_index_recommendation(self):
        rec = LLMIndexRecommendation(
            table_name="orders",
            index_name="idx_orders_user_id",
            index_definition="CREATE INDEX idx_orders_user_id ON orders(user_id)",
            rationale="Frequent lookups by user_id",
        )
        assert rec.queries_helped == []

    def test_prioritization_result(self):
        pr = LLMPrioritizationResult(
            ordered_detection_ids=[3, 1, 2], reasoning="Critical first"
        )
        assert pr.ordered_detection_ids[0] == 3


class TestConnectionStats:
    def test_defaults(self):
        cs = ConnectionStats()
        assert cs.max_connections == 100


class TestBgwriterStats:
    def test_defaults(self):
        bg = BgwriterStats()
        assert bg.checkpoints_timed == 0


class TestLockInfo:
    def test_construction(self):
        li = LockInfo(pid=1234, locktype="relation", mode="AccessExclusiveLock")
        assert li.granted
        assert li.blocked_by == []


class TestQueryStats:
    def test_defaults(self):
        qs = QueryStats()
        assert qs.calls == 0
        assert qs.queryid is None

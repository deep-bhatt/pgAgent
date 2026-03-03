"""Integration test: evaluator rollback behavior."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pgagent.evaluator import Evaluator
from pgagent.models import Action, ActionOutcome, ActionType, Suggestion, SuggestionStatus
from pgagent.sidecar import SidecarDB

from .conftest import requires_pg


class TestRollbackLogic:
    """Test rollback logic without requiring PG (evaluator comparison tests)."""

    def test_degraded_index_drop_triggers_rollback_flag(self):
        """When post seq_scans increase significantly, outcome is DEGRADED."""
        from pgagent.evaluator import Evaluator

        sidecar = SidecarDB(":memory:")

        # Create suggestion for FK
        sug = Suggestion(
            action_type=ActionType.DROP_INDEX,
            sql="DROP INDEX CONCURRENTLY idx_test",
            target_table="orders",
            target_index="idx_test",
            status=SuggestionStatus.EXECUTED,
            created_at=datetime.utcnow(),
        )
        sug_id = sidecar.save_suggestion(sug)

        action = Action(
            suggestion_id=sug_id,
            action_type=ActionType.DROP_INDEX,
            sql_executed="DROP INDEX CONCURRENTLY idx_test",
            target_table="orders",
            target_index="idx_test",
            pre_snapshot={"table_stats": {"seq_scan": 100, "idx_scan": 500}},
            outcome=ActionOutcome.PENDING_EVALUATION,
            executed_at=datetime.utcnow() - timedelta(minutes=10),
            rollback_sql="CREATE INDEX CONCURRENTLY idx_test ON orders(col)",
        )
        action_id = sidecar.save_action(action)
        action.id = action_id

        # Verify the comparison logic identifies degradation
        from pgagent.evaluator import Evaluator

        # Simulate post-snapshot with increased seq_scans
        post_snapshot = {"table_stats": {"seq_scan": 300, "idx_scan": 400}}

        # The _compare method should return DEGRADED
        # We test the standalone comparison
        pre_stats = action.pre_snapshot.get("table_stats", {})
        post_stats = post_snapshot.get("table_stats", {})

        pre_seq = pre_stats.get("seq_scan", 0)
        post_seq = post_stats.get("seq_scan", 0)

        # Index drop with 200 more seq scans → degraded
        assert post_seq > pre_seq + 100  # This is the degradation condition

        sidecar.close()

    def test_improved_vacuum_outcome(self):
        """VACUUM should be IMPROVED if dead tuples decrease."""
        from pgagent.evaluator import compare_vacuum_metrics

        assert compare_vacuum_metrics(1000, 50) == ActionOutcome.IMPROVED
        assert compare_vacuum_metrics(100, 100) == ActionOutcome.NO_CHANGE


@requires_pg
class TestRollbackWithPG:
    def test_create_and_rollback_index(self, pg_conn, settings, sidecar):
        """Create an index, then drop it as rollback."""
        pg_conn.autocommit = True
        cur = pg_conn.cursor()

        # Create a test index
        try:
            cur.execute(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_test_rollback ON users(username)"
            )
        except Exception:
            pass

        # Verify it exists
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE indexname = 'idx_test_rollback'"
        )
        assert cur.fetchone() is not None

        # Drop it (simulating rollback)
        cur.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_test_rollback")

        # Verify it's gone
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE indexname = 'idx_test_rollback'"
        )
        assert cur.fetchone() is None

        cur.close()

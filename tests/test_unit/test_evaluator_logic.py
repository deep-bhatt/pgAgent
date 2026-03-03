"""Tests for evaluator comparison logic and rollback SQL generation."""

from pgagent.evaluator import (
    compare_index_create_metrics,
    compare_vacuum_metrics,
    generate_rollback_sql,
)
from pgagent.models import Action, ActionOutcome, ActionType


class TestCompareVacuumMetrics:
    def test_improved_when_dead_tuples_decrease(self):
        assert compare_vacuum_metrics(1000, 50) == ActionOutcome.IMPROVED

    def test_no_change_when_same(self):
        assert compare_vacuum_metrics(100, 100) == ActionOutcome.NO_CHANGE

    def test_no_change_when_increase(self):
        assert compare_vacuum_metrics(100, 200) == ActionOutcome.NO_CHANGE


class TestCompareIndexCreateMetrics:
    def test_improved_when_idx_scans_increase_no_seq(self):
        result = compare_index_create_metrics(
            pre_seq=100, post_seq=100, pre_idx=50, post_idx=200
        )
        assert result == ActionOutcome.IMPROVED

    def test_improved_when_idx_delta_exceeds_seq_delta(self):
        result = compare_index_create_metrics(
            pre_seq=100, post_seq=110, pre_idx=50, post_idx=300
        )
        assert result == ActionOutcome.IMPROVED

    def test_success_when_idx_increase_but_seq_also(self):
        result = compare_index_create_metrics(
            pre_seq=100, post_seq=200, pre_idx=50, post_idx=100
        )
        assert result == ActionOutcome.SUCCESS

    def test_no_change_when_no_idx_increase(self):
        result = compare_index_create_metrics(
            pre_seq=100, post_seq=100, pre_idx=50, post_idx=50
        )
        assert result == ActionOutcome.NO_CHANGE


class TestGenerateRollbackSQL:
    def test_rollback_for_create_index(self):
        action = Action(
            suggestion_id=1,
            action_type=ActionType.CREATE_INDEX,
            target_index="idx_orders_user_id",
        )
        sql = generate_rollback_sql(action)
        assert sql is not None
        assert "DROP INDEX CONCURRENTLY" in sql
        assert "idx_orders_user_id" in sql

    def test_no_rollback_for_vacuum(self):
        action = Action(
            suggestion_id=1,
            action_type=ActionType.VACUUM,
            target_table="users",
        )
        assert generate_rollback_sql(action) is None

    def test_no_rollback_for_create_index_without_name(self):
        action = Action(
            suggestion_id=1,
            action_type=ActionType.CREATE_INDEX,
        )
        assert generate_rollback_sql(action) is None

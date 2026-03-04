"""Unit tests for SafetyValidator."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from pgagent.config import Settings
from pgagent.models import (
    Action,
    ActionOutcome,
    ActionType,
    RiskLevel,
    Suggestion,
    SuggestionStatus,
)
from pgagent.sidecar import SidecarDB
from pgagent.validator import SafetyValidator


# ── Helpers ───────────────────────────────────────────────────────────────


def _make_sidecar() -> SidecarDB:
    return SidecarDB(":memory:")


def _make_settings(**overrides) -> Settings:
    defaults = {
        "max_index_creates_per_hour": 3,
        "max_index_drops_per_hour": 2,
        "kill_threshold_per_cycle": 5,
        "rollback_cooldown_seconds": 1800,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _make_suggestion(
    action_type: ActionType = ActionType.CREATE_INDEX,
    sql: str = "CREATE INDEX CONCURRENTLY idx_test ON public.orders (col)",
    target_table: str = "public.orders",
    target_index: str | None = None,
    risk_level: RiskLevel = RiskLevel.MEDIUM,
) -> Suggestion:
    return Suggestion(
        id=1,
        detection_id=1,
        rule_id="test_rule",
        action_type=action_type,
        target_table=target_table,
        target_index=target_index,
        sql=sql,
        explanation="test",
        risk_level=risk_level,
        reversible=True,
        reverse_sql="",
        status=SuggestionStatus.APPROVED,
        created_at=datetime.utcnow(),
    )


def _save_snapshot_with_index(
    sidecar: SidecarDB,
    index_name: str,
    is_primary: bool = False,
    is_unique: bool = False,
) -> None:
    """Save a snapshot containing one index into the sidecar."""
    data = {
        "indexes": [
            {
                "index_name": index_name,
                "table_name": "orders",
                "schema_name": "public",
                "is_primary": is_primary,
                "is_unique": is_unique,
                "index_def": f"CREATE INDEX {index_name} ON public.orders (col)",
            }
        ]
    }
    sidecar.save_snapshot(datetime.utcnow(), data)


def _ensure_suggestion(sidecar: SidecarDB) -> int:
    """Ensure a suggestion exists in the sidecar so FK constraints pass."""
    existing = sidecar.get_suggestion(1)
    if existing:
        return 1
    sug = Suggestion(
        detection_id=None,
        rule_id="test",
        action_type=ActionType.VACUUM,
        sql="SELECT 1",
        explanation="test",
        status=SuggestionStatus.APPROVED,
        created_at=datetime.utcnow(),
    )
    return sidecar.save_suggestion(sug)


def _record_action(
    sidecar: SidecarDB,
    action_type: ActionType,
    outcome: ActionOutcome = ActionOutcome.SUCCESS,
    executed_at: datetime | None = None,
    rolled_back: bool = False,
    evaluated_at: datetime | None = None,
) -> int:
    """Record an action in the sidecar and return its id."""
    suggestion_id = _ensure_suggestion(sidecar)
    if executed_at is None:
        executed_at = datetime.utcnow()
    action = Action(
        suggestion_id=suggestion_id,
        action_type=action_type,
        sql_executed="SELECT 1",
        outcome=outcome,
        executed_at=executed_at,
        rolled_back=rolled_back,
        evaluated_at=evaluated_at,
    )
    return sidecar.save_action(action)


# ── Rule 1: No system catalog modification ───────────────────────────────


class TestSystemCatalogRejection:

    def test_rejects_pg_catalog_reference(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.VACUUM,
            sql="VACUUM pg_catalog.pg_class",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "system catalog" in reason

    def test_rejects_information_schema_reference(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.VACUUM,
            sql="ANALYZE information_schema.tables",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "system catalog" in reason

    def test_rejects_pg_prefix_tables(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.VACUUM,
            sql="VACUUM pg_stat_activity",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "system catalog" in reason

    def test_allows_normal_table(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.VACUUM,
            sql="VACUUM public.orders",
        )
        ok, reason = v.validate(sug)
        assert ok is True


# ── Rule 2: No PK / unique index drops ──────────────────────────────────


class TestPKUniqueDropRejection:

    def test_rejects_primary_key_drop(self) -> None:
        sidecar = _make_sidecar()
        _save_snapshot_with_index(sidecar, "orders_pkey", is_primary=True)
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.DROP_INDEX,
            sql="DROP INDEX CONCURRENTLY orders_pkey",
            target_index="orders_pkey",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "primary-key" in reason

    def test_rejects_unique_index_drop(self) -> None:
        sidecar = _make_sidecar()
        _save_snapshot_with_index(sidecar, "orders_email_uniq", is_unique=True)
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.DROP_INDEX,
            sql="DROP INDEX CONCURRENTLY orders_email_uniq",
            target_index="orders_email_uniq",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "unique" in reason

    def test_allows_normal_index_drop(self) -> None:
        sidecar = _make_sidecar()
        _save_snapshot_with_index(sidecar, "idx_orders_date", is_primary=False, is_unique=False)
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.DROP_INDEX,
            sql="DROP INDEX CONCURRENTLY idx_orders_date",
            target_index="idx_orders_date",
        )
        ok, reason = v.validate(sug)
        assert ok is True


# ── Rule 3: CONCURRENTLY enforcement ────────────────────────────────────


class TestConcurrentlyEnforcement:

    def test_rejects_create_index_without_concurrently(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.CREATE_INDEX,
            sql="CREATE INDEX idx_test ON public.orders (col)",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "CONCURRENTLY" in reason

    def test_rejects_drop_index_without_concurrently(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.DROP_INDEX,
            sql="DROP INDEX idx_test",
            target_index="idx_test",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "CONCURRENTLY" in reason

    def test_passes_create_index_with_concurrently(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.CREATE_INDEX,
            sql="CREATE INDEX CONCURRENTLY idx_test ON public.orders (col)",
        )
        ok, reason = v.validate(sug)
        assert ok is True

    def test_passes_drop_index_with_concurrently(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.DROP_INDEX,
            sql="DROP INDEX CONCURRENTLY idx_test",
            target_index="idx_test",
        )
        ok, reason = v.validate(sug)
        assert ok is True

    def test_non_index_action_does_not_need_concurrently(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())
        sug = _make_suggestion(
            action_type=ActionType.VACUUM,
            sql="VACUUM public.orders",
        )
        ok, reason = v.validate(sug)
        assert ok is True


# ── Rule 4: Index rate limiting ──────────────────────────────────────────


class TestIndexRateLimiting:

    def test_rejects_when_create_index_rate_exceeded(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(max_index_creates_per_hour=2)
        v = SafetyValidator(sidecar, settings)

        # Record 2 recent create_index actions (at the limit)
        for _ in range(2):
            _record_action(sidecar, ActionType.CREATE_INDEX)

        sug = _make_suggestion(
            action_type=ActionType.CREATE_INDEX,
            sql="CREATE INDEX CONCURRENTLY idx_new ON public.orders (col)",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "rate limit" in reason

    def test_allows_create_index_under_limit(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(max_index_creates_per_hour=3)
        v = SafetyValidator(sidecar, settings)

        _record_action(sidecar, ActionType.CREATE_INDEX)

        sug = _make_suggestion(
            action_type=ActionType.CREATE_INDEX,
            sql="CREATE INDEX CONCURRENTLY idx_new ON public.orders (col)",
        )
        ok, reason = v.validate(sug)
        assert ok is True

    def test_rejects_when_drop_index_rate_exceeded(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(max_index_drops_per_hour=1)
        v = SafetyValidator(sidecar, settings)

        _record_action(sidecar, ActionType.DROP_INDEX)

        sug = _make_suggestion(
            action_type=ActionType.DROP_INDEX,
            sql="DROP INDEX CONCURRENTLY idx_old",
            target_index="idx_old",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "rate limit" in reason


# ── Rule 5: Kill threshold ───────────────────────────────────────────────


class TestKillThreshold:

    def test_rejects_when_kill_threshold_reached(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(kill_threshold_per_cycle=2)
        v = SafetyValidator(sidecar, settings)

        for _ in range(2):
            _record_action(sidecar, ActionType.KILL_CONNECTION)

        sug = _make_suggestion(
            action_type=ActionType.KILL_CONNECTION,
            sql="SELECT pg_terminate_backend(1234)",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "threshold" in reason

    def test_allows_kill_under_threshold(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(kill_threshold_per_cycle=5)
        v = SafetyValidator(sidecar, settings)

        _record_action(sidecar, ActionType.KILL_CONNECTION)

        sug = _make_suggestion(
            action_type=ActionType.KILL_CONNECTION,
            sql="SELECT pg_terminate_backend(1234)",
        )
        ok, reason = v.validate(sug)
        assert ok is True


# ── Rule 6: Rollback cooldown ───────────────────────────────────────────


class TestRollbackCooldown:

    def test_rejects_mutation_during_rollback_cooldown(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(rollback_cooldown_seconds=1800)
        v = SafetyValidator(sidecar, settings)

        # Record a rolled-back action with evaluated_at = now
        action_id = _record_action(
            sidecar,
            ActionType.CREATE_INDEX,
            outcome=ActionOutcome.ROLLED_BACK,
            rolled_back=True,
            evaluated_at=datetime.utcnow(),
        )

        sug = _make_suggestion(
            action_type=ActionType.CREATE_INDEX,
            sql="CREATE INDEX CONCURRENTLY idx_new ON public.orders (col)",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "cooldown" in reason.lower()

    def test_allows_mutation_after_cooldown_expires(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(rollback_cooldown_seconds=60)
        v = SafetyValidator(sidecar, settings)

        # Record a rolled-back action from well in the past
        past = datetime.utcnow() - timedelta(seconds=120)
        _record_action(
            sidecar,
            ActionType.CREATE_INDEX,
            outcome=ActionOutcome.ROLLED_BACK,
            rolled_back=True,
            executed_at=past,
            evaluated_at=past,
        )

        sug = _make_suggestion(
            action_type=ActionType.CREATE_INDEX,
            sql="CREATE INDEX CONCURRENTLY idx_new ON public.orders (col)",
        )
        ok, reason = v.validate(sug)
        assert ok is True

    def test_non_mutation_not_affected_by_rollback_cooldown(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(rollback_cooldown_seconds=1800)
        v = SafetyValidator(sidecar, settings)

        _record_action(
            sidecar,
            ActionType.CREATE_INDEX,
            outcome=ActionOutcome.ROLLED_BACK,
            rolled_back=True,
            evaluated_at=datetime.utcnow(),
        )

        sug = _make_suggestion(
            action_type=ActionType.VACUUM,
            sql="VACUUM public.orders",
        )
        ok, reason = v.validate(sug)
        assert ok is True


# ── Rule 7: Concurrent mutation blocking ─────────────────────────────────


class TestConcurrentMutationBlocking:

    def test_rejects_mutation_when_another_active(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())

        # Record an action with pending_evaluation outcome (active mutation)
        _record_action(
            sidecar,
            ActionType.CREATE_INDEX,
            outcome=ActionOutcome.PENDING_EVALUATION,
        )

        sug = _make_suggestion(
            action_type=ActionType.DROP_INDEX,
            sql="DROP INDEX CONCURRENTLY idx_old",
            target_index="idx_old",
        )
        ok, reason = v.validate(sug)
        assert ok is False
        assert "in progress" in reason

    def test_allows_mutation_when_no_active(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())

        # Only completed actions, no pending
        _record_action(
            sidecar,
            ActionType.CREATE_INDEX,
            outcome=ActionOutcome.SUCCESS,
        )

        sug = _make_suggestion(
            action_type=ActionType.CREATE_INDEX,
            sql="CREATE INDEX CONCURRENTLY idx_new ON public.orders (col)",
        )
        ok, reason = v.validate(sug)
        assert ok is True

    def test_non_mutation_not_blocked(self) -> None:
        sidecar = _make_sidecar()
        v = SafetyValidator(sidecar, _make_settings())

        _record_action(
            sidecar,
            ActionType.CREATE_INDEX,
            outcome=ActionOutcome.PENDING_EVALUATION,
        )

        # VACUUM is not a mutation, should pass
        sug = _make_suggestion(
            action_type=ActionType.VACUUM,
            sql="VACUUM public.orders",
        )
        ok, reason = v.validate(sug)
        assert ok is True

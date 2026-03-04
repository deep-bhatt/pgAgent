"""Unit tests for SuggestionQueue."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pgagent.config import Settings
from pgagent.models import (
    Action,
    ActionOutcome,
    ActionType,
    Detection,
    DetectionType,
    RiskLevel,
    Severity,
    Suggestion,
    SuggestionStatus,
)
from pgagent.sidecar import SidecarDB
from pgagent.suggestion_queue import SuggestionQueue


# ── Helpers ───────────────────────────────────────────────────────────────


def _make_sidecar() -> SidecarDB:
    return SidecarDB(":memory:")


def _make_settings(**overrides) -> Settings:
    defaults = {
        "suggestion_ttl_seconds": 3600,
        "rejection_cooldown_seconds": 1800,
        "failure_cooldown_seconds": 3600,
        "auto_approve_low_risk": False,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _make_detection(
    target_table: str = "public.orders",
    target_index: str | None = None,
    detection_type: DetectionType = DetectionType.SEQ_SCAN_HEAVY,
) -> Detection:
    return Detection(
        id=None,
        detection_type=detection_type,
        severity=Severity.MEDIUM,
        target_table=target_table,
        target_index=target_index,
        message="test detection",
    )


def _add_default_suggestion(
    queue: SuggestionQueue,
    sidecar: SidecarDB,
    detection: Detection | None = None,
    action_type: ActionType = ActionType.CREATE_INDEX,
    risk_level: RiskLevel = RiskLevel.MEDIUM,
    rule_id: str = "rule_seq_scan_idx",
) -> Suggestion | None:
    if detection is None:
        detection = _make_detection()
    detection_id = sidecar.save_detection(detection)
    detection.id = detection_id
    return queue.add_suggestion(
        detection=detection,
        action_type=action_type,
        sql="CREATE INDEX CONCURRENTLY idx_orders_customer ON public.orders (customer_id)",
        explanation="Add index to speed up seq-scan heavy queries",
        risk_level=risk_level,
        reversible=True,
        reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS idx_orders_customer",
        rule_id=rule_id,
    )


# ── Tests ─────────────────────────────────────────────────────────────────


class TestAddAndDeduplication:
    """Adding suggestions and deduplication logic."""

    def test_add_suggestion_returns_suggestion_with_id(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        sug = _add_default_suggestion(queue, sidecar)

        assert sug is not None
        assert sug.id is not None
        assert sug.status == SuggestionStatus.PENDING

    def test_duplicate_returns_existing(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        first = _add_default_suggestion(queue, sidecar)
        second = _add_default_suggestion(queue, sidecar)

        assert first is not None
        assert second is not None
        assert first.id == second.id  # same suggestion returned

    def test_different_rule_id_not_deduplicated(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        first = _add_default_suggestion(queue, sidecar, rule_id="rule_a")
        second = _add_default_suggestion(queue, sidecar, rule_id="rule_b")

        assert first is not None
        assert second is not None
        assert first.id != second.id


class TestLifecycleTransitions:
    """pending -> approved -> executed -> evaluated."""

    def test_full_lifecycle(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        sug = _add_default_suggestion(queue, sidecar)
        assert sug is not None
        sid = sug.id

        # pending -> approved
        assert queue.approve(sid) is True
        assert sidecar.get_suggestion(sid).status == SuggestionStatus.APPROVED

        # approved -> executed
        assert queue.mark_executed(sid) is True
        assert sidecar.get_suggestion(sid).status == SuggestionStatus.EXECUTED

        # executed -> evaluated
        assert queue.mark_evaluated(sid) is True
        assert sidecar.get_suggestion(sid).status == SuggestionStatus.EVALUATED

    def test_approve_only_pending(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        sug = _add_default_suggestion(queue, sidecar)
        queue.approve(sug.id)

        # Approving an already-approved suggestion should fail
        assert queue.approve(sug.id) is False

    def test_reject_only_pending(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        sug = _add_default_suggestion(queue, sidecar)
        queue.approve(sug.id)

        # Rejecting an approved suggestion should fail
        assert queue.reject(sug.id) is False

    def test_mark_executed_only_approved(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        sug = _add_default_suggestion(queue, sidecar)

        # Still pending, not approved
        assert queue.mark_executed(sug.id) is False

    def test_mark_evaluated_only_executed(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        sug = _add_default_suggestion(queue, sidecar)
        queue.approve(sug.id)

        # Approved but not executed
        assert queue.mark_evaluated(sug.id) is False

    def test_reject_transitions_to_rejected(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())
        sug = _add_default_suggestion(queue, sidecar)

        assert queue.reject(sug.id) is True
        assert sidecar.get_suggestion(sug.id).status == SuggestionStatus.REJECTED


class TestRejectionCooldown:
    """After a rejection, re-suggestion is suppressed for cooldown period."""

    def test_rejection_cooldown_blocks_suggestion(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(rejection_cooldown_seconds=1800)
        queue = SuggestionQueue(sidecar, settings)

        sug = _add_default_suggestion(queue, sidecar)
        queue.reject(sug.id)

        # Same rule_id + target_table should be blocked
        result = _add_default_suggestion(queue, sidecar)
        assert result is None


class TestFailureCooldown:
    """After a failure, re-suggestion is suppressed for cooldown period."""

    def test_failure_cooldown_blocks_suggestion(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(failure_cooldown_seconds=3600)
        queue = SuggestionQueue(sidecar, settings)

        # Create and approve a suggestion, then record a failed action
        sug = _add_default_suggestion(queue, sidecar)
        queue.approve(sug.id)
        queue.mark_executed(sug.id)

        action = Action(
            suggestion_id=sug.id,
            action_type=ActionType.CREATE_INDEX,
            sql_executed=sug.sql,
            target_table=sug.target_table,
            outcome=ActionOutcome.FAILED,
            executed_at=datetime.utcnow(),
        )
        sidecar.save_action(action)

        # Now a new suggestion for the same action_type + target_table should be blocked
        result = _add_default_suggestion(queue, sidecar, rule_id="rule_different")
        assert result is None


class TestExpiry:
    """Stale suggestions should be expired."""

    def test_expire_stale_suggestions(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(suggestion_ttl_seconds=1)
        queue = SuggestionQueue(sidecar, settings)

        sug = _add_default_suggestion(queue, sidecar)
        assert sug is not None

        # Manually set expires_at to the past so we can expire it
        sidecar._conn.execute(
            "UPDATE suggestions SET expires_at = ? WHERE id = ?",
            ((datetime.utcnow() - timedelta(seconds=10)).isoformat(), sug.id),
        )
        sidecar._conn.commit()

        count = queue.expire_stale()
        assert count == 1

        refreshed = sidecar.get_suggestion(sug.id)
        assert refreshed.status == SuggestionStatus.EXPIRED


class TestAutoApprove:
    """Low-risk suggestions auto-approved when setting enabled."""

    def test_auto_approve_low_risk(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(auto_approve_low_risk=True)
        queue = SuggestionQueue(sidecar, settings)

        sug = _add_default_suggestion(queue, sidecar, risk_level=RiskLevel.LOW)
        assert sug is not None
        assert sug.status == SuggestionStatus.APPROVED
        assert sug.approved_at is not None

    def test_no_auto_approve_medium_risk(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(auto_approve_low_risk=True)
        queue = SuggestionQueue(sidecar, settings)

        sug = _add_default_suggestion(queue, sidecar, risk_level=RiskLevel.MEDIUM)
        assert sug is not None
        assert sug.status == SuggestionStatus.PENDING

    def test_no_auto_approve_when_disabled(self) -> None:
        sidecar = _make_sidecar()
        settings = _make_settings(auto_approve_low_risk=False)
        queue = SuggestionQueue(sidecar, settings)

        sug = _add_default_suggestion(queue, sidecar, risk_level=RiskLevel.LOW)
        assert sug is not None
        assert sug.status == SuggestionStatus.PENDING


class TestGetApproved:
    """get_approved returns only approved suggestions."""

    def test_get_approved_returns_only_approved(self) -> None:
        sidecar = _make_sidecar()
        queue = SuggestionQueue(sidecar, _make_settings())

        sug1 = _add_default_suggestion(queue, sidecar, rule_id="r1")
        sug2 = _add_default_suggestion(queue, sidecar, rule_id="r2")

        queue.approve(sug1.id)
        # sug2 remains pending

        approved = queue.get_approved()
        approved_ids = {s.id for s in approved}
        assert sug1.id in approved_ids
        assert sug2.id not in approved_ids

"""Suggestion lifecycle management for pgAgent."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from pgagent.config import Settings
from pgagent.models import (
    ActionType,
    Detection,
    RiskLevel,
    Suggestion,
    SuggestionStatus,
)
from pgagent.sidecar import SidecarDB

logger = logging.getLogger(__name__)


class SuggestionQueue:
    """Manages the full lifecycle of suggestions: creation, deduplication,
    cooldowns, auto-approval, state transitions, and expiry."""

    def __init__(self, sidecar: SidecarDB, settings: Settings) -> None:
        self._sidecar = sidecar
        self._settings = settings

    # ── Creation ──────────────────────────────────────────────────────

    def add_suggestion(
        self,
        detection: Detection,
        action_type: ActionType,
        sql: str,
        explanation: str,
        risk_level: RiskLevel,
        reversible: bool,
        reverse_sql: str,
        rule_id: str,
    ) -> Suggestion | None:
        """Create a new suggestion after checking deduplication and cooldowns.

        Returns the Suggestion with its assigned id, the existing duplicate
        Suggestion if deduplicated, or None if suppressed by a cooldown.
        """
        target_table = detection.target_table

        # -- Deduplication: same rule_id + target_table + action_type
        #    already pending or approved  -> skip, return existing
        existing = self._sidecar.find_duplicate_suggestion(
            rule_id, target_table, action_type
        )
        if existing is not None:
            logger.debug(
                "Duplicate suggestion suppressed: rule=%s table=%s action=%s (existing id=%s)",
                rule_id,
                target_table,
                action_type.value,
                existing.id,
            )
            return existing

        # -- Rejection cooldown: same rule_id + target_table was rejected
        #    within rejection_cooldown_seconds  -> skip
        last_rejection = self._sidecar.get_last_rejection_time(rule_id, target_table)
        if last_rejection is not None:
            cooldown_end = last_rejection + timedelta(
                seconds=self._settings.rejection_cooldown_seconds
            )
            if datetime.utcnow() < cooldown_end:
                logger.debug(
                    "Suggestion suppressed by rejection cooldown: rule=%s table=%s",
                    rule_id,
                    target_table,
                )
                return None

        # -- Failure cooldown: same action_type + target_table failed
        #    within failure_cooldown_seconds  -> skip
        last_failure = self._sidecar.get_last_failure_time(action_type, target_table)
        if last_failure is not None:
            cooldown_end = last_failure + timedelta(
                seconds=self._settings.failure_cooldown_seconds
            )
            if datetime.utcnow() < cooldown_end:
                logger.debug(
                    "Suggestion suppressed by failure cooldown: action=%s table=%s",
                    action_type.value,
                    target_table,
                )
                return None

        # -- Build suggestion
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=self._settings.suggestion_ttl_seconds)

        # Auto-approve low-risk suggestions when enabled
        if self._settings.auto_approve_low_risk and risk_level == RiskLevel.LOW:
            status = SuggestionStatus.APPROVED
            approved_at = now
        else:
            status = SuggestionStatus.PENDING
            approved_at = None

        suggestion = Suggestion(
            detection_id=detection.id,
            rule_id=rule_id,
            action_type=action_type,
            target_table=target_table,
            target_index=detection.target_index,
            target_pid=detection.target_pid,
            sql=sql,
            explanation=explanation,
            risk_level=risk_level,
            reversible=reversible,
            reverse_sql=reverse_sql,
            status=status,
            created_at=now,
            expires_at=expires_at,
            approved_at=approved_at,
        )

        suggestion_id = self._sidecar.save_suggestion(suggestion)
        suggestion.id = suggestion_id

        logger.info(
            "Suggestion created id=%d rule=%s action=%s status=%s risk=%s",
            suggestion_id,
            rule_id,
            action_type.value,
            status.value,
            risk_level.value,
        )
        return suggestion

    # ── State transitions ─────────────────────────────────────────────

    def approve(self, suggestion_id: int) -> bool:
        """Transition a suggestion from pending to approved."""
        suggestion = self._sidecar.get_suggestion(suggestion_id)
        if suggestion is None or suggestion.status != SuggestionStatus.PENDING:
            return False
        return self._sidecar.update_suggestion_status(
            suggestion_id, SuggestionStatus.APPROVED
        )

    def reject(self, suggestion_id: int) -> bool:
        """Transition a suggestion from pending to rejected."""
        suggestion = self._sidecar.get_suggestion(suggestion_id)
        if suggestion is None or suggestion.status != SuggestionStatus.PENDING:
            return False
        return self._sidecar.update_suggestion_status(
            suggestion_id, SuggestionStatus.REJECTED
        )

    def mark_executed(self, suggestion_id: int) -> bool:
        """Transition a suggestion from approved to executed."""
        suggestion = self._sidecar.get_suggestion(suggestion_id)
        if suggestion is None or suggestion.status != SuggestionStatus.APPROVED:
            return False
        return self._sidecar.update_suggestion_status(
            suggestion_id, SuggestionStatus.EXECUTED
        )

    def mark_evaluated(self, suggestion_id: int) -> bool:
        """Transition a suggestion from executed to evaluated."""
        suggestion = self._sidecar.get_suggestion(suggestion_id)
        if suggestion is None or suggestion.status != SuggestionStatus.EXECUTED:
            return False
        return self._sidecar.update_suggestion_status(
            suggestion_id, SuggestionStatus.EVALUATED
        )

    # ── Queries ───────────────────────────────────────────────────────

    def expire_stale(self) -> int:
        """Expire all pending suggestions past their TTL. Returns count expired."""
        count = self._sidecar.expire_stale_suggestions()
        if count > 0:
            logger.info("Expired %d stale suggestions", count)
        return count

    def get_approved(self) -> list[Suggestion]:
        """Get all approved suggestions ready for execution."""
        return self._sidecar.get_suggestions(status=SuggestionStatus.APPROVED)

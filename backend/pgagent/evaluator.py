"""Evaluator — assesses action outcomes and triggers rollback if needed."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import psycopg2

from pgagent.config import Settings
from pgagent.exceptions import RollbackError
from pgagent.models import Action, ActionOutcome, ActionType
from pgagent.sidecar import SidecarDB

logger = logging.getLogger(__name__)

# Evaluation delays per action type (seconds)
EVAL_DELAY_MAP = {
    ActionType.CREATE_INDEX: "eval_delay_create_index",
    ActionType.DROP_INDEX: "eval_delay_drop_index",
    ActionType.VACUUM: "eval_delay_vacuum",
    ActionType.ANALYZE: "eval_delay_analyze",
    ActionType.KILL_CONNECTION: "eval_delay_kill_connection",
}


class Evaluator:
    """Evaluates executed actions and auto-rolls back degraded ones."""

    def __init__(
        self,
        settings: Settings,
        conn: psycopg2.extensions.connection,
        sidecar: SidecarDB,
    ) -> None:
        self._settings = settings
        self._conn = conn
        self._sidecar = sidecar

    def get_eval_delay(self, action_type: ActionType) -> int:
        attr = EVAL_DELAY_MAP.get(action_type, "eval_delay_create_index")
        return getattr(self._settings, attr)

    def is_ready_for_eval(self, action: Action) -> bool:
        """Check if enough time has passed since execution for evaluation."""
        delay = self.get_eval_delay(action.action_type)
        elapsed = (datetime.utcnow() - action.executed_at).total_seconds()
        return elapsed >= delay

    def evaluate(self, action: Action) -> ActionOutcome:
        """Evaluate an action's outcome by comparing pre/post metrics."""
        if not self.is_ready_for_eval(action):
            return ActionOutcome.PENDING_EVALUATION

        post_snapshot = self._capture_post_snapshot(action)
        outcome = self._compare(action, post_snapshot)

        self._sidecar.update_action(
            action.id,  # type: ignore[arg-type]
            outcome=outcome,
            outcome_details=self._outcome_details(action, post_snapshot, outcome),
            post_snapshot=post_snapshot,
            evaluated_at=datetime.utcnow(),
        )

        if outcome == ActionOutcome.DEGRADED and action.rollback_sql:
            self._try_rollback(action)

        # Update suggestion status
        self._sidecar.update_suggestion_status(
            action.suggestion_id,
            __import__("pgagent.models", fromlist=["SuggestionStatus"]).SuggestionStatus.EVALUATED,
        )

        logger.info(
            "Action %d evaluated: %s", action.id, outcome.value
        )
        return outcome

    def evaluate_pending(self) -> list[tuple[int, ActionOutcome]]:
        """Evaluate all pending actions that are ready."""
        results = []
        for action in self._sidecar.get_pending_evaluations():
            if self.is_ready_for_eval(action):
                outcome = self.evaluate(action)
                results.append((action.id, outcome))  # type: ignore[arg-type]
        return results

    def _capture_post_snapshot(self, action: Action) -> dict[str, Any]:
        """Capture post-action metrics for comparison."""
        snapshot: dict[str, Any] = {"captured_at": datetime.utcnow().isoformat()}
        try:
            cur = self._conn.cursor()
            if action.target_table:
                cur.execute(
                    """SELECT seq_scan, idx_scan, n_live_tup, n_dead_tup
                       FROM pg_stat_user_tables WHERE relname = %s""",
                    (action.target_table,),
                )
                row = cur.fetchone()
                if row:
                    snapshot["table_stats"] = {
                        "seq_scan": row[0],
                        "idx_scan": row[1],
                        "n_live_tup": row[2],
                        "n_dead_tup": row[3],
                    }
            cur.close()
        except Exception as e:
            logger.warning("Failed to capture post-snapshot: %s", e)
        return snapshot

    def _compare(self, action: Action, post_snapshot: dict[str, Any]) -> ActionOutcome:
        """Compare pre and post snapshots to determine outcome."""
        pre = action.pre_snapshot
        post = post_snapshot

        if action.action_type == ActionType.VACUUM:
            return self._compare_vacuum(pre, post)
        elif action.action_type == ActionType.ANALYZE:
            return ActionOutcome.SUCCESS
        elif action.action_type == ActionType.CREATE_INDEX:
            return self._compare_index_create(pre, post)
        elif action.action_type == ActionType.DROP_INDEX:
            return self._compare_index_drop(pre, post)
        elif action.action_type == ActionType.KILL_CONNECTION:
            return ActionOutcome.SUCCESS

        return ActionOutcome.NO_CHANGE

    def _compare_vacuum(
        self, pre: dict[str, Any], post: dict[str, Any]
    ) -> ActionOutcome:
        pre_stats = pre.get("table_stats", {})
        post_stats = post.get("table_stats", {})

        pre_dead = pre_stats.get("n_dead_tup", 0)
        post_dead = post_stats.get("n_dead_tup", 0)

        if post_dead < pre_dead:
            return ActionOutcome.IMPROVED
        elif post_dead == pre_dead:
            return ActionOutcome.NO_CHANGE
        return ActionOutcome.NO_CHANGE

    def _compare_index_create(
        self, pre: dict[str, Any], post: dict[str, Any]
    ) -> ActionOutcome:
        pre_stats = pre.get("table_stats", {})
        post_stats = post.get("table_stats", {})

        pre_seq = pre_stats.get("seq_scan", 0)
        post_seq = post_stats.get("seq_scan", 0)
        pre_idx = pre_stats.get("idx_scan", 0)
        post_idx = post_stats.get("idx_scan", 0)

        seq_delta = post_seq - pre_seq
        idx_delta = post_idx - pre_idx

        if idx_delta > 0 and (seq_delta == 0 or idx_delta > seq_delta):
            return ActionOutcome.IMPROVED
        elif idx_delta > 0:
            return ActionOutcome.SUCCESS
        return ActionOutcome.NO_CHANGE

    def _compare_index_drop(
        self, pre: dict[str, Any], post: dict[str, Any]
    ) -> ActionOutcome:
        pre_stats = pre.get("table_stats", {})
        post_stats = post.get("table_stats", {})

        pre_seq = pre_stats.get("seq_scan", 0)
        post_seq = post_stats.get("seq_scan", 0)

        # If seq scans increased significantly after dropping an index, it's degraded
        if post_seq > pre_seq + 100:
            return ActionOutcome.DEGRADED
        return ActionOutcome.SUCCESS

    def _try_rollback(self, action: Action) -> None:
        """Attempt to rollback a degraded action."""
        if not action.rollback_sql:
            logger.warning("No rollback SQL for action %d", action.id)
            return
        try:
            old_autocommit = self._conn.autocommit
            self._conn.autocommit = True
            cur = self._conn.cursor()
            logger.info("Rolling back action %d: %s", action.id, action.rollback_sql)
            cur.execute(action.rollback_sql)
            cur.close()
            self._conn.autocommit = old_autocommit

            self._sidecar.update_action(
                action.id,  # type: ignore[arg-type]
                outcome=ActionOutcome.ROLLED_BACK,
                rolled_back=True,
                evaluated_at=datetime.utcnow(),
            )
            logger.info("Action %d rolled back successfully", action.id)
        except Exception as e:
            logger.error("Rollback failed for action %d: %s", action.id, e)
            raise RollbackError(f"Rollback failed: {e}") from e

    def _outcome_details(
        self, action: Action, post_snapshot: dict[str, Any], outcome: ActionOutcome
    ) -> str:
        pre = action.pre_snapshot.get("table_stats", {})
        post = post_snapshot.get("table_stats", {})
        parts = []
        for key in ["seq_scan", "idx_scan", "n_dead_tup"]:
            pv = pre.get(key, "?")
            av = post.get(key, "?")
            parts.append(f"{key}: {pv} -> {av}")
        return f"{outcome.value}: {', '.join(parts)}"


# ── Standalone comparison helpers for testing ──────────────────────────────


def compare_vacuum_metrics(pre_dead: int, post_dead: int) -> ActionOutcome:
    if post_dead < pre_dead:
        return ActionOutcome.IMPROVED
    return ActionOutcome.NO_CHANGE


def compare_index_create_metrics(
    pre_seq: int, post_seq: int, pre_idx: int, post_idx: int
) -> ActionOutcome:
    idx_delta = post_idx - pre_idx
    seq_delta = post_seq - pre_seq
    if idx_delta > 0 and (seq_delta == 0 or idx_delta > seq_delta):
        return ActionOutcome.IMPROVED
    elif idx_delta > 0:
        return ActionOutcome.SUCCESS
    return ActionOutcome.NO_CHANGE


def generate_rollback_sql(action: Action) -> str | None:
    """Generate rollback SQL for an action if reversible."""
    if action.action_type == ActionType.CREATE_INDEX and action.target_index:
        schema = "public"
        return f"DROP INDEX CONCURRENTLY IF EXISTS {schema}.{action.target_index}"
    return None

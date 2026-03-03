"""Executor — runs validated actions against PostgreSQL."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import psycopg2

from pgagent.config import Settings
from pgagent.exceptions import ExecutionError, SQLExecutionError
from pgagent.models import Action, ActionOutcome, ActionType, Suggestion
from pgagent.sidecar import SidecarDB
from pgagent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class Executor:
    """Executes approved suggestions against PostgreSQL."""

    def __init__(
        self,
        settings: Settings,
        conn: psycopg2.extensions.connection,
        sidecar: SidecarDB,
        registry: ToolRegistry,
    ) -> None:
        self._settings = settings
        self._conn = conn
        self._sidecar = sidecar
        self._registry = registry

    def execute(self, suggestion: Suggestion) -> Action:
        """Execute a suggestion. Returns the Action record.

        Captures pre-action snapshot, executes SQL via tool, logs to sidecar.
        No retry on failure.
        """
        tool = self._registry.get(suggestion.action_type)
        if tool is None:
            raise ExecutionError(f"No tool registered for {suggestion.action_type}")

        pre_snapshot = self._capture_pre_snapshot(suggestion)

        action = Action(
            suggestion_id=suggestion.id,  # type: ignore[arg-type]
            action_type=suggestion.action_type,
            sql_executed=suggestion.sql,
            target_table=suggestion.target_table,
            target_index=suggestion.target_index,
            target_pid=suggestion.target_pid,
            pre_snapshot=pre_snapshot,
            outcome=ActionOutcome.PENDING_EVALUATION,
            executed_at=datetime.utcnow(),
            rollback_sql=suggestion.reverse_sql,
        )

        try:
            logger.info(
                "Executing %s: %s", suggestion.action_type.value, suggestion.sql
            )
            tool.func(self._conn, suggestion.sql)
            action_id = self._sidecar.save_action(action)
            action.id = action_id
            logger.info("Action %d executed successfully", action_id)
        except SQLExecutionError as e:
            action.outcome = ActionOutcome.FAILED
            action.outcome_details = str(e)
            action.evaluated_at = datetime.utcnow()
            action_id = self._sidecar.save_action(action)
            action.id = action_id
            logger.error("Action failed: %s", e)

        return action

    def _capture_pre_snapshot(self, suggestion: Suggestion) -> dict[str, Any]:
        """Capture relevant metrics before executing the action."""
        snapshot: dict[str, Any] = {"captured_at": datetime.utcnow().isoformat()}

        try:
            cur = self._conn.cursor()

            if suggestion.target_table:
                cur.execute(
                    """SELECT seq_scan, idx_scan, n_live_tup, n_dead_tup
                       FROM pg_stat_user_tables
                       WHERE relname = %s""",
                    (suggestion.target_table,),
                )
                row = cur.fetchone()
                if row:
                    snapshot["table_stats"] = {
                        "seq_scan": row[0],
                        "idx_scan": row[1],
                        "n_live_tup": row[2],
                        "n_dead_tup": row[3],
                    }

            if suggestion.target_index and suggestion.action_type == ActionType.DROP_INDEX:
                cur.execute(
                    """SELECT idx_scan, idx_tup_read, idx_tup_fetch
                       FROM pg_stat_user_indexes
                       WHERE indexrelname = %s""",
                    (suggestion.target_index,),
                )
                row = cur.fetchone()
                if row:
                    snapshot["index_stats"] = {
                        "idx_scan": row[0],
                        "idx_tup_read": row[1],
                        "idx_tup_fetch": row[2],
                    }

            cur.close()
        except Exception as e:
            logger.warning("Failed to capture pre-snapshot: %s", e)

        return snapshot

"""SQLite sidecar database for pgAgent state persistence."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Any

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

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots(timestamp);

CREATE TABLE IF NOT EXISTS detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    detection_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'medium',
    target_table TEXT,
    target_index TEXT,
    target_pid INTEGER,
    message TEXT NOT NULL DEFAULT '',
    details TEXT NOT NULL DEFAULT '{}',
    llm_reasoning_needed INTEGER NOT NULL DEFAULT 0,
    detected_at TEXT NOT NULL,
    snapshot_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_detections_type ON detections(detection_type);
CREATE INDEX IF NOT EXISTS idx_detections_ts ON detections(detected_at);

CREATE TABLE IF NOT EXISTS suggestions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    detection_id INTEGER,
    rule_id TEXT NOT NULL DEFAULT '',
    action_type TEXT NOT NULL,
    target_table TEXT,
    target_index TEXT,
    target_pid INTEGER,
    sql TEXT NOT NULL DEFAULT '',
    explanation TEXT NOT NULL DEFAULT '',
    risk_level TEXT NOT NULL DEFAULT 'medium',
    reversible INTEGER NOT NULL DEFAULT 1,
    reverse_sql TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    expires_at TEXT,
    approved_at TEXT,
    rejected_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_suggestions_status ON suggestions(status);
CREATE INDEX IF NOT EXISTS idx_suggestions_rule ON suggestions(rule_id, target_table, action_type);

CREATE TABLE IF NOT EXISTS actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    suggestion_id INTEGER NOT NULL,
    action_type TEXT NOT NULL,
    sql_executed TEXT NOT NULL DEFAULT '',
    target_table TEXT,
    target_index TEXT,
    target_pid INTEGER,
    pre_snapshot TEXT NOT NULL DEFAULT '{}',
    post_snapshot TEXT NOT NULL DEFAULT '{}',
    outcome TEXT NOT NULL DEFAULT 'pending_evaluation',
    outcome_details TEXT NOT NULL DEFAULT '',
    executed_at TEXT NOT NULL,
    evaluated_at TEXT,
    rollback_sql TEXT NOT NULL DEFAULT '',
    rolled_back INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (suggestion_id) REFERENCES suggestions(id)
);
CREATE INDEX IF NOT EXISTS idx_actions_outcome ON actions(outcome);
CREATE INDEX IF NOT EXISTS idx_actions_ts ON actions(executed_at);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS index_scan_tracker (
    schema_name TEXT NOT NULL DEFAULT 'public',
    index_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_scan_count INTEGER NOT NULL DEFAULT 0,
    consecutive_zero_days INTEGER NOT NULL DEFAULT 0,
    last_checked TEXT NOT NULL,
    PRIMARY KEY (schema_name, index_name)
);

CREATE TABLE IF NOT EXISTS llm_failure_tracker (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    failed_at TEXT NOT NULL,
    error_type TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_llm_failures_ts ON llm_failure_tracker(failed_at);
"""


class SidecarDB:
    """Thread-safe SQLite sidecar for pgAgent state."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ── Snapshots ──────────────────────────────────────────────────────

    def save_snapshot(self, timestamp: datetime, data: dict[str, Any]) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO snapshots (timestamp, data) VALUES (?, ?)",
                (timestamp.isoformat(), json.dumps(data)),
            )
            self._conn.commit()
            return cur.lastrowid  # type: ignore[return-value]

    def get_recent_snapshots(self, hours: int = 1) -> list[dict[str, Any]]:
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, timestamp, data FROM snapshots WHERE timestamp >= ? ORDER BY timestamp",
                (cutoff,),
            ).fetchall()
        return [
            {"id": r["id"], "timestamp": r["timestamp"], "data": json.loads(r["data"])}
            for r in rows
        ]

    # ── Detections ─────────────────────────────────────────────────────

    def save_detection(self, det: Detection) -> int:
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO detections
                   (detection_type, severity, target_table, target_index, target_pid,
                    message, details, llm_reasoning_needed, detected_at, snapshot_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    det.detection_type.value,
                    det.severity.value,
                    det.target_table,
                    det.target_index,
                    det.target_pid,
                    det.message,
                    json.dumps(det.details),
                    1 if det.llm_reasoning_needed else 0,
                    det.detected_at.isoformat(),
                    det.snapshot_id,
                ),
            )
            self._conn.commit()
            return cur.lastrowid  # type: ignore[return-value]

    def get_recent_detections(self, hours: int = 24) -> list[Detection]:
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM detections WHERE detected_at >= ? ORDER BY detected_at DESC",
                (cutoff,),
            ).fetchall()
        return [self._row_to_detection(r) for r in rows]

    def _row_to_detection(self, row: sqlite3.Row) -> Detection:
        return Detection(
            id=row["id"],
            detection_type=DetectionType(row["detection_type"]),
            severity=Severity(row["severity"]),
            target_table=row["target_table"],
            target_index=row["target_index"],
            target_pid=row["target_pid"],
            message=row["message"],
            details=json.loads(row["details"]),
            llm_reasoning_needed=bool(row["llm_reasoning_needed"]),
            detected_at=datetime.fromisoformat(row["detected_at"]),
            snapshot_id=row["snapshot_id"],
        )

    # ── Suggestions ────────────────────────────────────────────────────

    def save_suggestion(self, sug: Suggestion) -> int:
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO suggestions
                   (detection_id, rule_id, action_type, target_table, target_index,
                    target_pid, sql, explanation, risk_level, reversible, reverse_sql,
                    status, created_at, expires_at, approved_at, rejected_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    sug.detection_id,
                    sug.rule_id,
                    sug.action_type.value,
                    sug.target_table,
                    sug.target_index,
                    sug.target_pid,
                    sug.sql,
                    sug.explanation,
                    sug.risk_level.value,
                    1 if sug.reversible else 0,
                    sug.reverse_sql,
                    sug.status.value,
                    sug.created_at.isoformat(),
                    sug.expires_at.isoformat() if sug.expires_at else None,
                    sug.approved_at.isoformat() if sug.approved_at else None,
                    sug.rejected_at.isoformat() if sug.rejected_at else None,
                ),
            )
            self._conn.commit()
            return cur.lastrowid  # type: ignore[return-value]

    def get_suggestion(self, suggestion_id: int) -> Suggestion | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM suggestions WHERE id = ?", (suggestion_id,)
            ).fetchone()
        if row is None:
            return None
        return self._row_to_suggestion(row)

    def get_suggestions(
        self, status: SuggestionStatus | None = None, limit: int = 100
    ) -> list[Suggestion]:
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM suggestions WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status.value, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM suggestions ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [self._row_to_suggestion(r) for r in rows]

    def update_suggestion_status(
        self, suggestion_id: int, status: SuggestionStatus
    ) -> bool:
        now = datetime.utcnow().isoformat()
        with self._lock:
            extra = ""
            params: list[Any] = [status.value]
            if status == SuggestionStatus.APPROVED:
                extra = ", approved_at = ?"
                params.append(now)
            elif status == SuggestionStatus.REJECTED:
                extra = ", rejected_at = ?"
                params.append(now)
            params.append(suggestion_id)
            cur = self._conn.execute(
                f"UPDATE suggestions SET status = ?{extra} WHERE id = ?",
                params,
            )
            self._conn.commit()
            return cur.rowcount > 0

    def find_duplicate_suggestion(
        self, rule_id: str, target_table: str | None, action_type: ActionType
    ) -> Suggestion | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT * FROM suggestions
                   WHERE rule_id = ? AND target_table IS ? AND action_type = ?
                     AND status IN ('pending', 'approved')
                   ORDER BY created_at DESC LIMIT 1""",
                (rule_id, target_table, action_type.value),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_suggestion(row)

    def expire_stale_suggestions(self) -> int:
        now = datetime.utcnow().isoformat()
        with self._lock:
            cur = self._conn.execute(
                """UPDATE suggestions SET status = 'expired'
                   WHERE status = 'pending' AND expires_at IS NOT NULL AND expires_at < ?""",
                (now,),
            )
            self._conn.commit()
            return cur.rowcount

    def _row_to_suggestion(self, row: sqlite3.Row) -> Suggestion:
        return Suggestion(
            id=row["id"],
            detection_id=row["detection_id"],
            rule_id=row["rule_id"],
            action_type=ActionType(row["action_type"]),
            target_table=row["target_table"],
            target_index=row["target_index"],
            target_pid=row["target_pid"],
            sql=row["sql"],
            explanation=row["explanation"],
            risk_level=RiskLevel(row["risk_level"]),
            reversible=bool(row["reversible"]),
            reverse_sql=row["reverse_sql"],
            status=SuggestionStatus(row["status"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            expires_at=datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None,
            approved_at=(
                datetime.fromisoformat(row["approved_at"]) if row["approved_at"] else None
            ),
            rejected_at=(
                datetime.fromisoformat(row["rejected_at"]) if row["rejected_at"] else None
            ),
        )

    # ── Actions ────────────────────────────────────────────────────────

    def save_action(self, action: Action) -> int:
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO actions
                   (suggestion_id, action_type, sql_executed, target_table, target_index,
                    target_pid, pre_snapshot, post_snapshot, outcome, outcome_details,
                    executed_at, evaluated_at, rollback_sql, rolled_back)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    action.suggestion_id,
                    action.action_type.value,
                    action.sql_executed,
                    action.target_table,
                    action.target_index,
                    action.target_pid,
                    json.dumps(action.pre_snapshot),
                    json.dumps(action.post_snapshot),
                    action.outcome.value,
                    action.outcome_details,
                    action.executed_at.isoformat(),
                    action.evaluated_at.isoformat() if action.evaluated_at else None,
                    action.rollback_sql,
                    1 if action.rolled_back else 0,
                ),
            )
            self._conn.commit()
            return cur.lastrowid  # type: ignore[return-value]

    def get_action(self, action_id: int) -> Action | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM actions WHERE id = ?", (action_id,)
            ).fetchone()
        if row is None:
            return None
        return self._row_to_action(row)

    def get_actions(
        self, outcome: ActionOutcome | None = None, limit: int = 100
    ) -> list[Action]:
        with self._lock:
            if outcome:
                rows = self._conn.execute(
                    "SELECT * FROM actions WHERE outcome = ? ORDER BY executed_at DESC LIMIT ?",
                    (outcome.value, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM actions ORDER BY executed_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [self._row_to_action(r) for r in rows]

    def get_pending_evaluations(self) -> list[Action]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM actions WHERE outcome = 'pending_evaluation' ORDER BY executed_at",
            ).fetchall()
        return [self._row_to_action(r) for r in rows]

    def update_action(
        self,
        action_id: int,
        outcome: ActionOutcome | None = None,
        outcome_details: str | None = None,
        post_snapshot: dict[str, Any] | None = None,
        evaluated_at: datetime | None = None,
        rolled_back: bool | None = None,
    ) -> bool:
        parts: list[str] = []
        params: list[Any] = []
        if outcome is not None:
            parts.append("outcome = ?")
            params.append(outcome.value)
        if outcome_details is not None:
            parts.append("outcome_details = ?")
            params.append(outcome_details)
        if post_snapshot is not None:
            parts.append("post_snapshot = ?")
            params.append(json.dumps(post_snapshot))
        if evaluated_at is not None:
            parts.append("evaluated_at = ?")
            params.append(evaluated_at.isoformat())
        if rolled_back is not None:
            parts.append("rolled_back = ?")
            params.append(1 if rolled_back else 0)
        if not parts:
            return False
        params.append(action_id)
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE actions SET {', '.join(parts)} WHERE id = ?", params
            )
            self._conn.commit()
            return cur.rowcount > 0

    def count_recent_actions(self, action_type: ActionType, hours: int = 1) -> int:
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        with self._lock:
            row = self._conn.execute(
                """SELECT COUNT(*) as cnt FROM actions
                   WHERE action_type = ? AND executed_at >= ?""",
                (action_type.value, cutoff),
            ).fetchone()
        return row["cnt"] if row else 0

    def has_active_mutation(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                """SELECT COUNT(*) as cnt FROM actions
                   WHERE outcome = 'pending_evaluation'
                     AND action_type IN ('create_index', 'drop_index')"""
            ).fetchone()
        return (row["cnt"] if row else 0) > 0

    def _row_to_action(self, row: sqlite3.Row) -> Action:
        return Action(
            id=row["id"],
            suggestion_id=row["suggestion_id"],
            action_type=ActionType(row["action_type"]),
            sql_executed=row["sql_executed"],
            target_table=row["target_table"],
            target_index=row["target_index"],
            target_pid=row["target_pid"],
            pre_snapshot=json.loads(row["pre_snapshot"]),
            post_snapshot=json.loads(row["post_snapshot"]),
            outcome=ActionOutcome(row["outcome"]),
            outcome_details=row["outcome_details"],
            executed_at=datetime.fromisoformat(row["executed_at"]),
            evaluated_at=(
                datetime.fromisoformat(row["evaluated_at"]) if row["evaluated_at"] else None
            ),
            rollback_sql=row["rollback_sql"],
            rolled_back=bool(row["rolled_back"]),
        )

    # ── Index scan tracker ─────────────────────────────────────────────

    def upsert_index_scan(
        self,
        schema_name: str,
        index_name: str,
        table_name: str,
        scan_count: int,
    ) -> None:
        now = datetime.utcnow().isoformat()
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM index_scan_tracker WHERE schema_name = ? AND index_name = ?",
                (schema_name, index_name),
            ).fetchone()
            if existing is None:
                self._conn.execute(
                    """INSERT INTO index_scan_tracker
                       (schema_name, index_name, table_name, first_seen,
                        last_scan_count, consecutive_zero_days, last_checked)
                       VALUES (?, ?, ?, ?, ?, 0, ?)""",
                    (schema_name, index_name, table_name, now, scan_count, now),
                )
            else:
                prev_count = existing["last_scan_count"]
                zero_days = existing["consecutive_zero_days"]
                if scan_count <= prev_count:
                    zero_days += 1
                else:
                    zero_days = 0
                self._conn.execute(
                    """UPDATE index_scan_tracker
                       SET last_scan_count = ?, consecutive_zero_days = ?, last_checked = ?
                       WHERE schema_name = ? AND index_name = ?""",
                    (scan_count, zero_days, now, schema_name, index_name),
                )
            self._conn.commit()

    def get_unused_indexes(self, min_zero_days: int = 7) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM index_scan_tracker
                   WHERE consecutive_zero_days >= ?""",
                (min_zero_days,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── LLM failure tracker ────────────────────────────────────────────

    def record_llm_failure(self, error_type: str, error_message: str) -> int:
        now = datetime.utcnow().isoformat()
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO llm_failure_tracker (failed_at, error_type, error_message) VALUES (?, ?, ?)",
                (now, error_type, error_message),
            )
            self._conn.commit()
            return cur.lastrowid  # type: ignore[return-value]

    def get_consecutive_llm_failures(self) -> int:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM llm_failure_tracker ORDER BY failed_at DESC LIMIT 10"
            ).fetchall()
        return len(rows)

    def clear_llm_failures(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM llm_failure_tracker")
            self._conn.commit()

    # ── Config ─────────────────────────────────────────────────────────

    def set_config(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                (key, value),
            )
            self._conn.commit()

    def get_config(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM config WHERE key = ?", (key,)
            ).fetchone()
        return row["value"] if row else None

    # ── Retention / Pruning ────────────────────────────────────────────

    def prune(self, settings: Settings) -> dict[str, int]:
        """Remove old data based on retention settings. Returns counts deleted."""
        counts = {}
        now = datetime.utcnow()

        snap_cutoff = (now - timedelta(hours=settings.snapshot_retention_hours)).isoformat()
        det_cutoff = (now - timedelta(hours=settings.detection_retention_hours)).isoformat()
        act_cutoff = (now - timedelta(hours=settings.action_retention_hours)).isoformat()

        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM snapshots WHERE timestamp < ?", (snap_cutoff,)
            )
            counts["snapshots"] = cur.rowcount
            cur = self._conn.execute(
                "DELETE FROM detections WHERE detected_at < ?", (det_cutoff,)
            )
            counts["detections"] = cur.rowcount
            cur = self._conn.execute(
                "DELETE FROM actions WHERE executed_at < ?", (act_cutoff,)
            )
            counts["actions"] = cur.rowcount
            # Prune suggestions that have been terminal for longer than action retention
            cur = self._conn.execute(
                """DELETE FROM suggestions
                   WHERE status IN ('expired', 'rejected', 'evaluated')
                     AND created_at < ?""",
                (act_cutoff,),
            )
            counts["suggestions"] = cur.rowcount
            self._conn.commit()
        return counts

    def get_last_rejection_time(
        self, rule_id: str, target_table: str | None
    ) -> datetime | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT rejected_at FROM suggestions
                   WHERE rule_id = ? AND target_table IS ? AND status = 'rejected'
                   ORDER BY rejected_at DESC LIMIT 1""",
                (rule_id, target_table),
            ).fetchone()
        if row and row["rejected_at"]:
            return datetime.fromisoformat(row["rejected_at"])
        return None

    def get_last_failure_time(
        self, action_type: ActionType, target_table: str | None
    ) -> datetime | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT executed_at FROM actions
                   WHERE action_type = ? AND target_table IS ?
                     AND outcome IN ('failed', 'rolled_back')
                   ORDER BY executed_at DESC LIMIT 1""",
                (action_type.value, target_table),
            ).fetchone()
        if row and row["executed_at"]:
            return datetime.fromisoformat(row["executed_at"])
        return None

    def get_last_rollback_time(self) -> datetime | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT evaluated_at FROM actions
                   WHERE rolled_back = 1
                   ORDER BY evaluated_at DESC LIMIT 1"""
            ).fetchone()
        if row and row["evaluated_at"]:
            return datetime.fromisoformat(row["evaluated_at"])
        return None

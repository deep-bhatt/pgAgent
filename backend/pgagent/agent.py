"""Agent — main orchestration loop for pgAgent."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler

from pgagent.config import Settings
from pgagent.detector import Detector
from pgagent.evaluator import Evaluator
from pgagent.exceptions import (
    ExecutionError,
    ObserverError,
    PgAgentError,
    SafetyValidationError,
)
from pgagent.executor import Executor
from pgagent.models import (
    ActionType,
    Detection,
    RiskLevel,
    Suggestion,
    SuggestionStatus,
)
from pgagent.observer import Observer
from pgagent.reasoner import Reasoner
from pgagent.sidecar import SidecarDB
from pgagent.suggestion_queue import SuggestionQueue
from pgagent.tools.registry import create_default_registry
from pgagent.validator import SafetyValidator

if TYPE_CHECKING:
    from pgagent.models import Action, SnapshotWithDeltas

logger = logging.getLogger(__name__)

# Maps detection types to action types + SQL templates
_DETECTION_ACTION_MAP: dict[str, dict[str, Any]] = {
    "vacuum_dead_tuples": {
        "action_type": ActionType.VACUUM,
        "sql_template": "VACUUM {table}",
        "risk_level": RiskLevel.LOW,
        "reversible": False,
    },
    "vacuum_stale": {
        "action_type": ActionType.VACUUM,
        "sql_template": "VACUUM {table}",
        "risk_level": RiskLevel.LOW,
        "reversible": False,
    },
    "analyze_stale": {
        "action_type": ActionType.ANALYZE,
        "sql_template": "ANALYZE {table}",
        "risk_level": RiskLevel.LOW,
        "reversible": False,
    },
    "table_bloat": {
        "action_type": ActionType.VACUUM,
        "sql_template": "VACUUM FULL {table}",
        "risk_level": RiskLevel.MEDIUM,
        "reversible": False,
    },
    "idle_in_transaction": {
        "action_type": ActionType.KILL_CONNECTION,
        "sql_template": "SELECT pg_terminate_backend({pid})",
        "risk_level": RiskLevel.MEDIUM,
        "reversible": False,
    },
    "unused_index": {
        "action_type": ActionType.DROP_INDEX,
        "sql_template": "DROP INDEX CONCURRENTLY IF EXISTS {index}",
        "risk_level": RiskLevel.HIGH,
        "reversible": False,
    },
}


class Agent:
    """Main pgAgent orchestrator."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._sidecar = SidecarDB(settings.sidecar_db_path)
        self._scheduler = BackgroundScheduler()
        self._conn: psycopg2.extensions.connection | None = None
        self._observer: Observer | None = None
        self._detector = Detector(settings)
        self._reasoner = Reasoner(settings, self._sidecar)
        self._queue = SuggestionQueue(self._sidecar, settings)
        self._validator = SafetyValidator(self._sidecar, settings)
        self._registry = create_default_registry()
        self._executor: Executor | None = None
        self._evaluator: Evaluator | None = None

        self._paused = False
        self._cycle_count = 0
        self._last_cycle_at: datetime | None = None
        self._start_time: float | None = None
        self._event_handlers: dict[str, list[Callable]] = {
            "detection": [],
            "suggestion": [],
            "action": [],
            "evaluation": [],
            "snapshot": [],
        }

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def sidecar(self) -> SidecarDB:
        return self._sidecar

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def cycle_count(self) -> int:
        return self._cycle_count

    @property
    def last_cycle_at(self) -> datetime | None:
        return self._last_cycle_at

    @property
    def uptime_seconds(self) -> float:
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time

    @property
    def pg_connected(self) -> bool:
        if self._conn is None:
            return False
        try:
            return self._conn.closed == 0
        except Exception:
            return False

    # ── Event handlers ─────────────────────────────────────────────────

    def on_event(self, event_type: str, handler: Callable) -> None:
        """Register an event handler for broadcasting."""
        if event_type in self._event_handlers:
            self._event_handlers[event_type].append(handler)

    def _emit(self, event_type: str, data: Any) -> None:
        for handler in self._event_handlers.get(event_type, []):
            try:
                handler(data)
            except Exception as e:
                logger.warning("Event handler error (%s): %s", event_type, e)

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start(self) -> None:
        """Connect to PG, start the scheduler."""
        self._start_time = time.time()
        self._connect()
        self._scheduler.add_job(
            self._cycle,
            "interval",
            seconds=self._settings.observe_interval_seconds,
            id="agent_cycle",
            max_instances=1,
        )
        self._scheduler.start()
        logger.info("Agent started, cycle interval=%ds", self._settings.observe_interval_seconds)

    def stop(self) -> None:
        """Stop the scheduler and close connections."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        if self._conn and not self._conn.closed:
            self._conn.close()
        self._sidecar.close()
        logger.info("Agent stopped")

    def pause(self) -> None:
        self._paused = True
        logger.info("Agent paused")

    def resume(self) -> None:
        self._paused = False
        logger.info("Agent resumed")

    def _connect(self) -> None:
        """Establish PG connection and initialize components."""
        try:
            self._conn = psycopg2.connect(self._settings.pg_dsn)
            self._observer = Observer(self._settings, self._conn, self._sidecar)
            self._observer.check_connection()
            self._executor = Executor(
                self._settings, self._conn, self._sidecar, self._registry
            )
            self._evaluator = Evaluator(self._settings, self._conn, self._sidecar)
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL: %s", e)
            raise

    # ── Main cycle ─────────────────────────────────────────────────────

    def _cycle(self) -> None:
        """One observe → detect → reason → suggest → evaluate cycle."""
        if self._paused:
            return

        try:
            # 1. Observe
            swd = self._observer.observe()  # type: ignore[union-attr]
            self._emit("snapshot", {"timestamp": swd.snapshot.timestamp.isoformat()})

            # 2. Detect
            detections = self._detector.detect(swd)
            for det in detections:
                det.id = self._sidecar.save_detection(det)
                self._emit("detection", {
                    "id": det.id,
                    "type": det.detection_type.value,
                    "severity": det.severity.value,
                    "message": det.message,
                })

            # 3. Reason (LLM for seq_scan_heavy, prioritize all)
            llm_detections = self._detector.needs_llm(detections)
            direct_detections = self._detector.direct(detections)

            for det in llm_detections:
                self._handle_llm_detection(det, swd)

            # 4. Suggest (direct detections → suggestion queue)
            for det in direct_detections:
                self._create_suggestion_from_detection(det)

            # 5. Expire stale suggestions
            expired = self._queue.expire_stale()
            if expired:
                logger.info("Expired %d stale suggestions", expired)

            # 6. Evaluate pending actions
            if self._evaluator:
                results = self._evaluator.evaluate_pending()
                for action_id, outcome in results:
                    self._emit("evaluation", {
                        "action_id": action_id,
                        "outcome": outcome.value,
                    })

            # 7. Prune old data
            self._sidecar.prune(self._settings)

            self._cycle_count += 1
            self._last_cycle_at = datetime.utcnow()

        except ObserverError as e:
            logger.error("Observer error in cycle: %s", e)
        except PgAgentError as e:
            logger.error("Agent cycle error: %s", e)
        except Exception as e:
            logger.exception("Unexpected error in agent cycle: %s", e)

    def _handle_llm_detection(
        self, detection: Detection, swd: SnapshotWithDeltas
    ) -> None:
        """Use LLM to get index recommendations for seq_scan_heavy detections."""
        if not detection.target_table:
            return

        table_info = self._gather_table_info(detection.target_table, swd)
        try:
            recommendations = self._reasoner.recommend_indexes(detection, table_info)
            for rec in recommendations:
                self._queue.add_suggestion(
                    detection=detection,
                    action_type=ActionType.CREATE_INDEX,
                    sql=rec.index_definition,
                    explanation=rec.rationale,
                    risk_level=RiskLevel.MEDIUM,
                    reversible=True,
                    reverse_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {rec.index_name}",
                    rule_id=f"llm_index_{detection.target_table}",
                )
        except Exception as e:
            logger.warning("LLM recommendation failed for %s: %s", detection.target_table, e)

    def _gather_table_info(
        self, table_name: str, swd: SnapshotWithDeltas
    ) -> dict[str, Any]:
        """Gather context for LLM prompt from snapshot + PG queries."""
        info: dict[str, Any] = {
            "table_columns": [],
            "existing_indexes": [],
            "slow_queries": [],
            "table_stats": {},
        }

        # Table stats from snapshot
        for ts in swd.snapshot.tables:
            if ts.table_name == table_name:
                info["table_stats"] = {
                    "seq_scan": ts.seq_scan,
                    "idx_scan": ts.idx_scan,
                    "n_live_tup": ts.n_live_tup,
                    "n_dead_tup": ts.n_dead_tup,
                    "table_size_bytes": ts.table_size_bytes,
                }
                break

        # Indexes from snapshot
        for idx in swd.snapshot.indexes:
            if idx.table_name == table_name:
                info["existing_indexes"].append({
                    "index_name": idx.index_name,
                    "index_def": idx.index_def,
                })

        # Query helper tools (require connection)
        if self._conn and not self._conn.closed:
            from pgagent.tools.query_tools import get_table_columns, get_table_queries

            info["table_columns"] = get_table_columns(self._conn, "public", table_name)
            if swd.snapshot.has_pg_stat_statements:
                info["slow_queries"] = get_table_queries(self._conn, table_name)

        return info

    def _create_suggestion_from_detection(self, detection: Detection) -> None:
        """Create a suggestion from a direct (non-LLM) detection."""
        dt = detection.detection_type.value
        mapping = _DETECTION_ACTION_MAP.get(dt)
        if not mapping:
            return

        action_type = mapping["action_type"]
        table = detection.target_table or ""
        index = detection.target_index or ""
        pid = detection.target_pid or 0

        sql = mapping["sql_template"].format(table=table, index=index, pid=pid)
        reverse_sql = ""
        if action_type == ActionType.DROP_INDEX and index:
            # We can't easily reconstruct index def for reverse, leave empty
            pass

        sug = self._queue.add_suggestion(
            detection=detection,
            action_type=action_type,
            sql=sql,
            explanation=detection.message,
            risk_level=mapping["risk_level"],
            reversible=mapping.get("reversible", False),
            reverse_sql=reverse_sql,
            rule_id=dt,
        )
        if sug:
            self._emit("suggestion", {
                "id": sug.id,
                "action_type": sug.action_type.value,
                "target_table": sug.target_table,
                "status": sug.status.value,
                "explanation": sug.explanation,
            })

    # ── Public API methods (called by REST endpoints) ──────────────────

    def approve(self, suggestion_id: int) -> Suggestion | None:
        """Approve a suggestion and attempt execution."""
        ok = self._queue.approve(suggestion_id)
        if not ok:
            return None

        suggestion = self._sidecar.get_suggestion(suggestion_id)
        if suggestion is None:
            return None

        # Validate safety
        is_valid, reason = self._validator.validate(suggestion)
        if not is_valid:
            self._queue.reject(suggestion_id)
            suggestion = self._sidecar.get_suggestion(suggestion_id)
            if suggestion:
                suggestion.explanation += f" [Rejected by safety: {reason}]"
            return suggestion

        # Execute
        self._execute_suggestion(suggestion)
        return self._sidecar.get_suggestion(suggestion_id)

    def reject(self, suggestion_id: int) -> Suggestion | None:
        """Reject a suggestion."""
        self._queue.reject(suggestion_id)
        return self._sidecar.get_suggestion(suggestion_id)

    def _execute_suggestion(self, suggestion: Suggestion) -> None:
        """Execute an approved suggestion."""
        if self._executor is None:
            logger.error("Executor not initialized")
            return

        try:
            action = self._executor.execute(suggestion)
            self._queue.mark_executed(suggestion.id)  # type: ignore[arg-type]
            self._emit("action", {
                "id": action.id,
                "action_type": action.action_type.value,
                "sql": action.sql_executed,
                "outcome": action.outcome.value,
            })
        except ExecutionError as e:
            logger.error("Execution failed for suggestion %s: %s", suggestion.id, e)

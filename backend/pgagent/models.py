"""Pydantic models and enums for pgAgent."""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ── Enums ──────────────────────────────────────────────────────────────────


class DetectionType(str, enum.Enum):
    VACUUM_DEAD_TUPLES = "vacuum_dead_tuples"
    VACUUM_STALE = "vacuum_stale"
    ANALYZE_STALE = "analyze_stale"
    UNUSED_INDEX = "unused_index"
    SEQ_SCAN_HEAVY = "seq_scan_heavy"
    IDLE_IN_TRANSACTION = "idle_in_transaction"
    LOCK_CONTENTION = "lock_contention"
    CONNECTION_SATURATION = "connection_saturation"
    TABLE_BLOAT = "table_bloat"
    HIGH_BACKEND_WRITES = "high_backend_writes"
    FORCED_CHECKPOINTS = "forced_checkpoints"


class Severity(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SuggestionStatus(str, enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    EXECUTED = "executed"
    EVALUATED = "evaluated"


class ActionType(str, enum.Enum):
    CREATE_INDEX = "create_index"
    DROP_INDEX = "drop_index"
    VACUUM = "vacuum"
    ANALYZE = "analyze"
    KILL_CONNECTION = "kill_connection"


class ActionOutcome(str, enum.Enum):
    SUCCESS = "success"
    IMPROVED = "improved"
    NO_CHANGE = "no_change"
    DEGRADED = "degraded"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    PENDING_EVALUATION = "pending_evaluation"


class RiskLevel(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# ── Snapshot models ────────────────────────────────────────────────────────


class TableStats(BaseModel):
    schema_name: str = "public"
    table_name: str
    n_live_tup: int = 0
    n_dead_tup: int = 0
    seq_scan: int = 0
    seq_tup_read: int = 0
    idx_scan: int = 0
    idx_tup_fetch: int = 0
    n_tup_ins: int = 0
    n_tup_upd: int = 0
    n_tup_del: int = 0
    n_tup_hot_upd: int = 0
    last_vacuum: datetime | None = None
    last_autovacuum: datetime | None = None
    last_analyze: datetime | None = None
    last_autoanalyze: datetime | None = None
    vacuum_count: int = 0
    autovacuum_count: int = 0
    analyze_count: int = 0
    autoanalyze_count: int = 0
    table_size_bytes: int = 0


class IndexStats(BaseModel):
    schema_name: str = "public"
    table_name: str
    index_name: str
    idx_scan: int = 0
    idx_tup_read: int = 0
    idx_tup_fetch: int = 0
    index_size_bytes: int = 0
    is_unique: bool = False
    is_primary: bool = False
    index_def: str = ""


class ConnectionStats(BaseModel):
    total_connections: int = 0
    active: int = 0
    idle: int = 0
    idle_in_transaction: int = 0
    waiting: int = 0
    max_connections: int = 100


class QueryStats(BaseModel):
    queryid: int | None = None
    query: str = ""
    calls: int = 0
    total_exec_time: float = 0.0
    mean_exec_time: float = 0.0
    rows: int = 0
    shared_blks_hit: int = 0
    shared_blks_read: int = 0


class BgwriterStats(BaseModel):
    checkpoints_timed: int = 0
    checkpoints_req: int = 0
    buffers_checkpoint: int = 0
    buffers_clean: int = 0
    maxwritten_clean: int = 0
    buffers_backend: int = 0
    buffers_alloc: int = 0


class LockInfo(BaseModel):
    pid: int
    locktype: str = ""
    mode: str = ""
    granted: bool = True
    relation: str | None = None
    wait_event_type: str | None = None
    wait_event: str | None = None
    state: str = ""
    query: str = ""
    wait_duration_seconds: float | None = None
    blocked_by: list[int] = Field(default_factory=list)


class Snapshot(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    tables: list[TableStats] = Field(default_factory=list)
    indexes: list[IndexStats] = Field(default_factory=list)
    connections: ConnectionStats = Field(default_factory=ConnectionStats)
    queries: list[QueryStats] = Field(default_factory=list)
    bgwriter: BgwriterStats = Field(default_factory=BgwriterStats)
    locks: list[LockInfo] = Field(default_factory=list)
    pg_version: int = 16
    has_pg_stat_statements: bool = False


# ── Delta models ───────────────────────────────────────────────────────────


class TableDeltas(BaseModel):
    schema_name: str = "public"
    table_name: str
    seq_scan_delta: int = 0
    idx_scan_delta: int = 0
    tup_ins_delta: int = 0
    tup_upd_delta: int = 0
    tup_del_delta: int = 0
    dead_tup_delta: int = 0


class IndexHistory(BaseModel):
    schema_name: str = "public"
    table_name: str
    index_name: str
    idx_scan_delta: int = 0


class SnapshotWithDeltas(BaseModel):
    snapshot: Snapshot
    table_deltas: list[TableDeltas] = Field(default_factory=list)
    index_history: list[IndexHistory] = Field(default_factory=list)


# ── Detection ──────────────────────────────────────────────────────────────


class Detection(BaseModel):
    id: int | None = None
    detection_type: DetectionType
    severity: Severity = Severity.MEDIUM
    target_table: str | None = None
    target_index: str | None = None
    target_pid: int | None = None
    message: str = ""
    details: dict[str, Any] = Field(default_factory=dict)
    llm_reasoning_needed: bool = False
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    snapshot_id: int | None = None


# ── Suggestion ─────────────────────────────────────────────────────────────


class Suggestion(BaseModel):
    id: int | None = None
    detection_id: int | None = None
    rule_id: str = ""
    action_type: ActionType
    target_table: str | None = None
    target_index: str | None = None
    target_pid: int | None = None
    sql: str = ""
    explanation: str = ""
    risk_level: RiskLevel = RiskLevel.MEDIUM
    reversible: bool = True
    reverse_sql: str = ""
    status: SuggestionStatus = SuggestionStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None
    approved_at: datetime | None = None
    rejected_at: datetime | None = None


# ── Action ─────────────────────────────────────────────────────────────────


class Action(BaseModel):
    id: int | None = None
    suggestion_id: int
    action_type: ActionType
    sql_executed: str = ""
    target_table: str | None = None
    target_index: str | None = None
    target_pid: int | None = None
    pre_snapshot: dict[str, Any] = Field(default_factory=dict)
    post_snapshot: dict[str, Any] = Field(default_factory=dict)
    outcome: ActionOutcome = ActionOutcome.PENDING_EVALUATION
    outcome_details: str = ""
    executed_at: datetime = Field(default_factory=datetime.utcnow)
    evaluated_at: datetime | None = None
    rollback_sql: str = ""
    rolled_back: bool = False


# ── LLM response models ───────────────────────────────────────────────────


class LLMIndexRecommendation(BaseModel):
    table_name: str
    index_name: str
    index_definition: str
    rationale: str = ""
    estimated_impact: str = ""
    queries_helped: list[str] = Field(default_factory=list)


class LLMPrioritizationResult(BaseModel):
    ordered_detection_ids: list[int] = Field(default_factory=list)
    reasoning: str = ""

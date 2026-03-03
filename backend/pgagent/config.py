"""Configuration via environment variables with PGAGENT_ prefix."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "PGAGENT_"}

    # PostgreSQL connection
    pg_dsn: str = "postgresql://localhost:5432/postgres"

    # Groq / LLM
    groq_api_key: str = ""
    llm_model: str = "llama-3.3-70b-versatile"
    llm_timeout_seconds: int = 30
    llm_max_consecutive_failures: int = 3

    # Observer
    observe_interval_seconds: int = 60
    delta_lookback_seconds: int = 3600
    min_pg_version: int = 13

    # Detection thresholds
    dead_tuple_ratio_threshold: float = 0.05
    vacuum_stale_hours: int = 24
    analyze_stale_hours: int = 24
    unused_index_days: int = 7
    unused_index_min_size_bytes: int = 1_048_576  # 1 MB
    seq_scan_ratio_threshold: float = 0.5
    seq_scan_min_total: int = 100
    idle_transaction_seconds: int = 300
    lock_wait_seconds: int = 30
    connection_saturation_ratio: float = 0.8
    table_bloat_ratio_threshold: float = 0.3
    bgwriter_maxwritten_ratio: float = 0.5
    checkpoint_warning_ratio: float = 0.5

    # Suggestion queue
    suggestion_ttl_seconds: int = 3600
    rejection_cooldown_seconds: int = 1800
    failure_cooldown_seconds: int = 3600
    auto_approve_low_risk: bool = False

    # Safety
    max_index_creates_per_hour: int = 3
    max_index_drops_per_hour: int = 2
    kill_threshold_per_cycle: int = 5
    rollback_cooldown_seconds: int = 1800
    max_concurrent_mutations: int = 1

    # Evaluation delays (seconds)
    eval_delay_create_index: int = 300
    eval_delay_drop_index: int = 300
    eval_delay_vacuum: int = 120
    eval_delay_analyze: int = 60
    eval_delay_kill_connection: int = 60

    # Retention
    snapshot_retention_hours: int = 168  # 7 days
    detection_retention_hours: int = 168
    action_retention_hours: int = 720  # 30 days

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8420

    # Sidecar DB
    sidecar_db_path: str = "pgagent_sidecar.db"

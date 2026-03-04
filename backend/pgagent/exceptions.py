"""Exception hierarchy for pgAgent."""


class PgAgentError(Exception):
    """Base exception for all pgAgent errors."""


# --- Connection errors ---


class ConnectionError(PgAgentError):
    """Base for connection-related errors."""


class PostgresConnectionError(ConnectionError):
    """Failed to connect to PostgreSQL."""


class InsufficientPrivilegesError(ConnectionError):
    """Connected but lacking required privileges."""


class UnsupportedVersionError(ConnectionError):
    """PostgreSQL version is below minimum required (13+)."""


# --- Observer errors ---


class ObserverError(PgAgentError):
    """Base for observer/snapshot errors."""


class SnapshotCollectionError(ObserverError):
    """Failed to collect a snapshot from system views."""


class DeltaComputationError(ObserverError):
    """Failed to compute deltas between snapshots."""


# --- LLM errors ---


class LLMError(PgAgentError):
    """Base for LLM/Groq interaction errors."""


class LLMConnectionError(LLMError):
    """Failed to reach the LLM provider."""


class LLMResponseParseError(LLMError):
    """LLM response could not be parsed into expected format."""


class LLMRateLimitError(LLMError):
    """LLM rate limit exceeded."""


class LLMTimeoutError(LLMError):
    """LLM request timed out."""


# --- Execution errors ---


class ExecutionError(PgAgentError):
    """Base for action execution errors."""


class SQLExecutionError(ExecutionError):
    """SQL statement failed during execution."""


class SafetyValidationError(ExecutionError):
    """Action rejected by safety validator."""


class ConcurrentMutationError(ExecutionError):
    """Another mutation is already in progress."""


# --- Evaluation errors ---


class EvaluationError(PgAgentError):
    """Base for evaluation/rollback errors."""


class RollbackError(EvaluationError):
    """Rollback action failed."""


class MetricCollectionError(EvaluationError):
    """Failed to collect post-action metrics for evaluation."""

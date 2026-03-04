"""Prompt templates for LLM-powered analysis in pgAgent."""

from __future__ import annotations

# ── Index Recommendation Prompts ──────────────────────────────────────────

INDEX_RECOMMENDATION_SYSTEM = (
    "You are a PostgreSQL performance expert. Analyze the provided table schema, "
    "existing indexes, and query patterns to recommend optimal indexes. "
    "Respond ONLY with valid JSON."
)

INDEX_RECOMMENDATION_USER = """\
Analyze the following PostgreSQL table and recommend indexes to improve performance.

## Table: {table_name}

### Columns:
{table_columns_formatted}

### Existing Indexes:
{existing_indexes_formatted}

### Slow Queries (with stats):
{slow_queries_formatted}

### Table Statistics:
- Sequential scans: {seq_scan}
- Index scans: {idx_scan}
- Live rows: {n_live_tup}
- Dead rows: {n_dead_tup}
- Table size: {table_size_bytes} bytes

Respond with a JSON object containing a single key "recommendations", which is a \
list of objects. Each object must have these fields:
- "index_name": a descriptive name for the proposed index (string)
- "index_definition": the full CREATE INDEX statement (string)
- "rationale": why this index helps (string)
- "estimated_impact": qualitative impact estimate, e.g. "high", "medium", "low" (string)
- "queries_helped": list of query strings from the slow queries above that benefit (list of strings)

If no indexes are recommended, return {{"recommendations": []}}.
"""

# ── Prioritization Prompts ────────────────────────────────────────────────

PRIORITIZATION_SYSTEM = (
    "You are a PostgreSQL operations expert. Given multiple detected issues, "
    "prioritize them by urgency and impact. Respond ONLY with valid JSON."
)

PRIORITIZATION_USER = """\
The following issues have been detected in a PostgreSQL database. \
Prioritize them by urgency and potential impact.

## Detected Issues:
{detections_formatted}

Respond with a JSON object containing:
- "ordered_detection_ids": a list of detection IDs (integers), ordered from most \
urgent to least urgent
- "reasoning": a brief explanation of the prioritization rationale (string)
"""


# ── Formatting helpers ────────────────────────────────────────────────────


def format_columns(columns: list[str]) -> str:
    """Format a list of column names/definitions for the prompt."""
    if not columns:
        return "  (no column information available)"
    return "\n".join(f"  - {col}" for col in columns)


def format_indexes(indexes: list[str]) -> str:
    """Format a list of existing index definitions for the prompt."""
    if not indexes:
        return "  (no existing indexes)"
    return "\n".join(f"  - {idx}" for idx in indexes)


def format_slow_queries(queries: list[dict[str, str | float | int]]) -> str:
    """Format slow queries with their statistics for the prompt.

    Each query dict should have keys like: query, calls, mean_exec_time, rows.
    """
    if not queries:
        return "  (no slow query data available)"
    parts: list[str] = []
    for i, q in enumerate(queries, 1):
        query_str = q.get("query", "(unknown)")
        calls = q.get("calls", "?")
        mean_time = q.get("mean_exec_time", "?")
        rows = q.get("rows", "?")
        parts.append(
            f"  {i}. Query: {query_str}\n"
            f"     Calls: {calls}, Mean time: {mean_time}ms, Rows: {rows}"
        )
    return "\n".join(parts)


def format_detections(
    detections: list[dict[str, str | int]],
) -> str:
    """Format detection summaries for the prioritization prompt.

    Each detection dict should have keys: id, detection_type, severity, message, details.
    """
    if not detections:
        return "  (no detections)"
    parts: list[str] = []
    for det in detections:
        det_id = det.get("id", "?")
        det_type = det.get("detection_type", "unknown")
        severity = det.get("severity", "unknown")
        message = det.get("message", "")
        target = det.get("target_table", "")
        target_str = f" on {target}" if target else ""
        parts.append(
            f"  - ID {det_id}: [{severity.upper()}] {det_type}{target_str}\n"
            f"    {message}"
        )
    return "\n".join(parts)


def build_index_recommendation_prompt(
    table_name: str,
    table_columns: list[str],
    existing_indexes: list[str],
    slow_queries: list[dict[str, str | float | int]],
    table_stats: dict[str, int],
) -> list[dict[str, str]]:
    """Build the full message list for an index recommendation LLM call.

    Returns a list of message dicts with 'role' and 'content' keys,
    suitable for passing to the Groq chat completions API.
    """
    user_content = INDEX_RECOMMENDATION_USER.format(
        table_name=table_name,
        table_columns_formatted=format_columns(table_columns),
        existing_indexes_formatted=format_indexes(existing_indexes),
        slow_queries_formatted=format_slow_queries(slow_queries),
        seq_scan=table_stats.get("seq_scan", 0),
        idx_scan=table_stats.get("idx_scan", 0),
        n_live_tup=table_stats.get("n_live_tup", 0),
        n_dead_tup=table_stats.get("n_dead_tup", 0),
        table_size_bytes=table_stats.get("table_size_bytes", 0),
    )
    return [
        {"role": "system", "content": INDEX_RECOMMENDATION_SYSTEM},
        {"role": "user", "content": user_content},
    ]


def build_prioritization_prompt(
    detections: list[dict[str, str | int]],
) -> list[dict[str, str]]:
    """Build the full message list for a detection prioritization LLM call.

    Returns a list of message dicts with 'role' and 'content' keys,
    suitable for passing to the Groq chat completions API.
    """
    user_content = PRIORITIZATION_USER.format(
        detections_formatted=format_detections(detections),
    )
    return [
        {"role": "system", "content": PRIORITIZATION_SYSTEM},
        {"role": "user", "content": user_content},
    ]

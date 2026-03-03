"""Query statistics routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

router = APIRouter(prefix="/api", tags=["queries"])


@router.get("/queries")
async def list_queries(request: Request) -> dict[str, Any]:
    """Return recent query stats from the latest snapshot."""
    agent = request.app.state.agent
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is None:
        return {"queries": [], "has_pg_stat_statements": False}

    # Retrieve the most recent snapshot (last 1 hour, take the latest)
    recent = sidecar.get_recent_snapshots(hours=1)
    if not recent:
        return {"queries": [], "has_pg_stat_statements": False}

    latest = recent[-1]
    data = latest.get("data", {})

    queries = data.get("queries", [])
    has_pg_stat_statements = data.get("has_pg_stat_statements", False)

    return {
        "queries": queries,
        "has_pg_stat_statements": has_pg_stat_statements,
        "snapshot_timestamp": latest.get("timestamp", ""),
    }

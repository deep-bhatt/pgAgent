"""Health and agent status routes."""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Request

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health_check(request: Request) -> dict[str, Any]:
    """Return overall health status of the agent."""
    agent = request.app.state.agent
    start_time: float = getattr(request.app.state, "start_time", time.time())
    uptime_seconds = round(time.time() - start_time, 2)

    # Determine if the agent loop is running
    agent_running: bool = getattr(agent, "running", False)

    # Check PG connectivity
    pg_connected = False
    try:
        conn = getattr(agent, "_conn", None)
        if conn is not None and not conn.closed:
            pg_connected = True
    except Exception:
        pass

    return {
        "status": "healthy",
        "agent_running": agent_running,
        "pg_connected": pg_connected,
        "uptime_seconds": uptime_seconds,
    }


@router.get("/health/history")
async def health_history(request: Request) -> dict[str, Any]:
    """Return recent snapshots from the sidecar (last 1 hour)."""
    agent = request.app.state.agent
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is None:
        return {"snapshots": []}

    snapshots = sidecar.get_recent_snapshots(hours=1)
    return {"snapshots": snapshots}


@router.get("/status")
async def agent_status(request: Request) -> dict[str, Any]:
    """Return agent operational status."""
    agent = request.app.state.agent

    paused: bool = getattr(agent, "paused", False)
    cycle_count: int = getattr(agent, "cycle_count", 0)
    last_cycle_at: str = ""
    raw_last_cycle = getattr(agent, "last_cycle_at", None)
    if raw_last_cycle is not None:
        last_cycle_at = raw_last_cycle.isoformat() if hasattr(raw_last_cycle, "isoformat") else str(raw_last_cycle)

    # Count detections in last 24h
    detections_24h = 0
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is not None:
        try:
            recent_detections = sidecar.get_recent_detections(hours=24)
            detections_24h = len(recent_detections)
        except Exception:
            pass

    # Count pending suggestions
    pending_suggestions = 0
    if sidecar is not None:
        try:
            from pgagent.models import SuggestionStatus

            pending = sidecar.get_suggestions(status=SuggestionStatus.PENDING)
            pending_suggestions = len(pending)
        except Exception:
            pass

    return {
        "paused": paused,
        "cycle_count": cycle_count,
        "last_cycle_at": last_cycle_at,
        "detections_24h": detections_24h,
        "pending_suggestions": pending_suggestions,
    }


@router.post("/agent/pause")
async def pause_agent(request: Request) -> dict[str, Any]:
    """Pause the agent loop."""
    agent = request.app.state.agent
    agent.pause()
    return {"status": "paused"}


@router.post("/agent/resume")
async def resume_agent(request: Request) -> dict[str, Any]:
    """Resume the agent loop."""
    agent = request.app.state.agent
    agent.resume()
    return {"status": "resumed"}

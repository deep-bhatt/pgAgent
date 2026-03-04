"""Suggestion management routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from pgagent.models import SuggestionStatus

router = APIRouter(prefix="/api", tags=["suggestions"])


@router.get("/suggestions")
async def list_suggestions(
    request: Request,
    status: str | None = Query(default=None, description="Filter by status, e.g. 'pending'"),
) -> dict[str, Any]:
    """List suggestions, optionally filtered by status."""
    agent = request.app.state.agent
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is None:
        return {"suggestions": []}

    filter_status: SuggestionStatus | None = None
    if status is not None:
        try:
            filter_status = SuggestionStatus(status)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status value: {status}. Valid values: {[s.value for s in SuggestionStatus]}",
            )

    suggestions = sidecar.get_suggestions(status=filter_status)
    return {"suggestions": [s.model_dump(mode="json") for s in suggestions]}


@router.get("/suggestions/{suggestion_id}")
async def get_suggestion(request: Request, suggestion_id: int) -> dict[str, Any]:
    """Get a single suggestion by ID."""
    agent = request.app.state.agent
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is None:
        raise HTTPException(status_code=503, detail="Sidecar database not available")

    suggestion = sidecar.get_suggestion(suggestion_id)
    if suggestion is None:
        raise HTTPException(status_code=404, detail=f"Suggestion {suggestion_id} not found")

    return {"suggestion": suggestion.model_dump(mode="json")}


@router.post("/suggestions/{suggestion_id}/approve")
async def approve_suggestion(request: Request, suggestion_id: int) -> dict[str, Any]:
    """Approve a pending suggestion."""
    agent = request.app.state.agent
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is None:
        raise HTTPException(status_code=503, detail="Sidecar database not available")

    suggestion = sidecar.get_suggestion(suggestion_id)
    if suggestion is None:
        raise HTTPException(status_code=404, detail=f"Suggestion {suggestion_id} not found")

    if suggestion.status != SuggestionStatus.PENDING:
        raise HTTPException(
            status_code=409,
            detail=f"Suggestion {suggestion_id} is not pending (current status: {suggestion.status.value})",
        )

    updated = agent.approve(suggestion_id)
    if updated is None:
        raise HTTPException(status_code=500, detail="Failed to approve suggestion")

    return {"suggestion": updated.model_dump(mode="json")}


@router.post("/suggestions/{suggestion_id}/reject")
async def reject_suggestion(request: Request, suggestion_id: int) -> dict[str, Any]:
    """Reject a pending suggestion."""
    agent = request.app.state.agent
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is None:
        raise HTTPException(status_code=503, detail="Sidecar database not available")

    suggestion = sidecar.get_suggestion(suggestion_id)
    if suggestion is None:
        raise HTTPException(status_code=404, detail=f"Suggestion {suggestion_id} not found")

    if suggestion.status != SuggestionStatus.PENDING:
        raise HTTPException(
            status_code=409,
            detail=f"Suggestion {suggestion_id} is not pending (current status: {suggestion.status.value})",
        )

    updated = agent.reject(suggestion_id)
    if updated is None:
        raise HTTPException(status_code=500, detail="Failed to reject suggestion")

    return {"suggestion": updated.model_dump(mode="json")}

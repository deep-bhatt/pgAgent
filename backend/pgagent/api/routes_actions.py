"""Action management routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from pgagent.models import ActionOutcome

router = APIRouter(prefix="/api", tags=["actions"])


@router.get("/actions")
async def list_actions(
    request: Request,
    outcome: str | None = Query(default=None, description="Filter by outcome, e.g. 'pending_evaluation'"),
) -> dict[str, Any]:
    """List actions, optionally filtered by outcome."""
    agent = request.app.state.agent
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is None:
        return {"actions": []}

    filter_outcome: ActionOutcome | None = None
    if outcome is not None:
        try:
            filter_outcome = ActionOutcome(outcome)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid outcome value: {outcome}. Valid values: {[o.value for o in ActionOutcome]}",
            )

    actions = sidecar.get_actions(outcome=filter_outcome)
    return {"actions": [a.model_dump(mode="json") for a in actions]}


@router.get("/actions/{action_id}")
async def get_action(request: Request, action_id: int) -> dict[str, Any]:
    """Get a single action by ID."""
    agent = request.app.state.agent
    sidecar = getattr(agent, "_sidecar", None)
    if sidecar is None:
        raise HTTPException(status_code=503, detail="Sidecar database not available")

    action = sidecar.get_action(action_id)
    if action is None:
        raise HTTPException(status_code=404, detail=f"Action {action_id} not found")

    return {"action": action.model_dump(mode="json")}

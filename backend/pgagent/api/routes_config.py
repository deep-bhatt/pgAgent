"""Runtime configuration routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/api", tags=["config"])


@router.get("/config")
async def get_config(request: Request) -> dict[str, Any]:
    """Return the current runtime configuration as JSON."""
    agent = request.app.state.agent
    settings = getattr(agent, "_settings", None)
    if settings is None:
        raise HTTPException(status_code=503, detail="Settings not available")

    return {"config": settings.model_dump(mode="json")}


@router.put("/config")
async def update_config(request: Request) -> dict[str, Any]:
    """Update runtime configuration values.

    Accepts a JSON body of key-value pairs. Only known settings fields
    are updated; unknown keys are ignored. Sensitive fields (e.g. API keys)
    cannot be updated via this endpoint.
    """
    agent = request.app.state.agent
    settings = getattr(agent, "_settings", None)
    if settings is None:
        raise HTTPException(status_code=503, detail="Settings not available")

    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")

    # Fields that cannot be changed at runtime
    protected_fields = {"pg_dsn", "groq_api_key", "sidecar_db_path", "api_host", "api_port"}

    updated_keys: list[str] = []
    rejected_keys: list[str] = []
    unknown_keys: list[str] = []

    model_fields = settings.model_fields

    for key, value in body.items():
        if key in protected_fields:
            rejected_keys.append(key)
            continue
        if key not in model_fields:
            unknown_keys.append(key)
            continue

        try:
            setattr(settings, key, value)
            updated_keys.append(key)
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid value for '{key}': {exc}",
            )

    return {
        "updated": updated_keys,
        "rejected_protected": rejected_keys,
        "unknown": unknown_keys,
        "config": settings.model_dump(mode="json"),
    }

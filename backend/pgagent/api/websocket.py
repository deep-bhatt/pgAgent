"""WebSocket connection manager and routes for pgAgent."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

logger = logging.getLogger(__name__)

CHANNELS = {"metrics", "suggestions", "actions"}

ws_router = APIRouter(tags=["websocket"])


class ConnectionManager:
    """Manages WebSocket connections across multiple channels."""

    def __init__(self) -> None:
        self._connections: dict[str, list[WebSocket]] = {ch: [] for ch in CHANNELS}

    async def connect(self, websocket: WebSocket, channel: str) -> None:
        """Accept a WebSocket connection and register it to a channel."""
        await websocket.accept()
        if channel not in self._connections:
            self._connections[channel] = []
        self._connections[channel].append(websocket)
        logger.debug("WebSocket connected to channel '%s' (%d total)", channel, len(self._connections[channel]))

    async def disconnect(self, websocket: WebSocket, channel: str) -> None:
        """Remove a WebSocket connection from a channel."""
        if channel in self._connections:
            try:
                self._connections[channel].remove(websocket)
            except ValueError:
                pass
            logger.debug("WebSocket disconnected from channel '%s' (%d remaining)", channel, len(self._connections[channel]))

    async def broadcast(self, channel: str, data: Any) -> None:
        """Broadcast data to all connected clients on a channel."""
        if channel not in self._connections:
            return

        message = json.dumps(data) if not isinstance(data, str) else data
        disconnected: list[WebSocket] = []

        for ws in self._connections[channel]:
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_text(message)
            except Exception:
                disconnected.append(ws)

        # Clean up disconnected clients
        for ws in disconnected:
            try:
                self._connections[channel].remove(ws)
            except ValueError:
                pass

    def connection_count(self, channel: str | None = None) -> int:
        """Return the number of active connections, optionally for a specific channel."""
        if channel is not None:
            return len(self._connections.get(channel, []))
        return sum(len(conns) for conns in self._connections.values())


# Singleton manager instance shared across the application
manager = ConnectionManager()


def broadcast_event(mgr: ConnectionManager, channel: str, data: Any) -> None:
    """Bridge function to broadcast from synchronous code (e.g. APScheduler).

    Uses ``asyncio.run_coroutine_threadsafe`` to schedule the broadcast
    on the running event loop. Safe to call from non-async threads.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(mgr.broadcast(channel, data), loop)
        else:
            loop.run_until_complete(mgr.broadcast(channel, data))
    except RuntimeError:
        # No event loop available — try to get the loop from the running thread
        try:
            loop = asyncio.get_running_loop()
            asyncio.run_coroutine_threadsafe(mgr.broadcast(channel, data), loop)
        except RuntimeError:
            logger.warning("No event loop available for WebSocket broadcast on channel '%s'", channel)


# ── WebSocket route handlers ──────────────────────────────────────────────


async def _ws_handler(websocket: WebSocket, channel: str) -> None:
    """Generic WebSocket handler for a given channel."""
    if channel not in CHANNELS:
        await websocket.close(code=4001, reason=f"Unknown channel: {channel}")
        return

    await manager.connect(websocket, channel)
    try:
        while True:
            # Keep the connection alive by reading incoming messages
            # Clients can send pings or other messages; we just discard them
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket, channel)
    except Exception:
        await manager.disconnect(websocket, channel)


@ws_router.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket) -> None:
    """WebSocket endpoint for real-time metrics updates."""
    await _ws_handler(websocket, "metrics")


@ws_router.websocket("/ws/suggestions")
async def ws_suggestions(websocket: WebSocket) -> None:
    """WebSocket endpoint for real-time suggestion updates."""
    await _ws_handler(websocket, "suggestions")


@ws_router.websocket("/ws/actions")
async def ws_actions(websocket: WebSocket) -> None:
    """WebSocket endpoint for real-time action updates."""
    await _ws_handler(websocket, "actions")

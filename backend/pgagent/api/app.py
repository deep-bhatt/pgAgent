"""FastAPI application factory for pgAgent."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from pgagent.api.routes_actions import router as actions_router
from pgagent.api.routes_config import router as config_router
from pgagent.api.routes_health import router as health_router
from pgagent.api.routes_queries import router as queries_router
from pgagent.api.routes_suggestions import router as suggestions_router
from pgagent.api.websocket import manager, ws_router

if TYPE_CHECKING:
    from pgagent.agent import Agent

logger = logging.getLogger(__name__)


def get_agent(request: Request) -> Agent:
    """Retrieve the Agent instance stored in app state."""
    return request.app.state.agent  # type: ignore[return-value]


def create_app(agent: Agent) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        agent: The Agent instance to attach to app state.

    Returns:
        Configured FastAPI application.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.agent = agent
        app.state.start_time = time.time()
        app.state.ws_manager = manager
        logger.info("pgAgent API started")
        yield
        logger.info("pgAgent API shutting down")

    app = FastAPI(
        title="pgAgent",
        description="PostgreSQL monitoring and optimization agent API",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS middleware — allow all origins for development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include route routers
    app.include_router(health_router)
    app.include_router(suggestions_router)
    app.include_router(actions_router)
    app.include_router(queries_router)
    app.include_router(config_router)
    app.include_router(ws_router)

    return app

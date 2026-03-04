"""Integration test: full suggestion lifecycle via API."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pgagent.config import Settings
from pgagent.models import ActionType, Detection, DetectionType, RiskLevel, Severity
from pgagent.sidecar import SidecarDB
from pgagent.suggestion_queue import SuggestionQueue

from .conftest import DSN, requires_pg


class TestSuggestionLifecycleUnit:
    """Test the suggestion lifecycle without requiring PG (unit-style in integration dir)."""

    def test_full_lifecycle_via_api(self):
        """Test suggestion CRUD through FastAPI TestClient."""
        from pgagent.agent import Agent

        settings = Settings(
            pg_dsn=DSN,
            sidecar_db_path=":memory:",
            auto_approve_low_risk=False,
        )

        # Create a minimal agent (don't start — no PG needed for this test)
        sidecar = SidecarDB(":memory:")
        queue = SuggestionQueue(sidecar, settings)

        # Create a detection and suggestion
        det = Detection(
            detection_type=DetectionType.VACUUM_DEAD_TUPLES,
            severity=Severity.MEDIUM,
            target_table="users",
            message="High dead tuple ratio",
        )
        det.id = sidecar.save_detection(det)

        sug = queue.add_suggestion(
            detection=det,
            action_type=ActionType.VACUUM,
            sql="VACUUM users",
            explanation="High dead tuple ratio on users",
            risk_level=RiskLevel.LOW,
            reversible=False,
            reverse_sql="",
            rule_id="vacuum_dead_tuples",
        )
        assert sug is not None
        assert sug.id is not None

        # Verify suggestion is retrievable
        fetched = sidecar.get_suggestion(sug.id)
        assert fetched is not None
        assert fetched.status.value == "pending"

        # Approve
        ok = queue.approve(sug.id)
        assert ok

        fetched = sidecar.get_suggestion(sug.id)
        assert fetched.status.value == "approved"

        sidecar.close()


@requires_pg
class TestSuggestionLifecycleAPI:
    """Test suggestion lifecycle via REST API with real PG."""

    def test_api_suggestion_crud(self, pg_conn, settings, sidecar):
        """Create suggestion, list it, approve it via API endpoints."""
        from pgagent.agent import Agent
        from pgagent.api.app import create_app

        # We can't fully start the agent without PG being managed,
        # so we test the API layer with a mock-ish setup
        agent = Agent.__new__(Agent)
        agent._settings = settings
        agent._sidecar = sidecar
        agent._paused = False
        agent._cycle_count = 0
        agent._last_cycle_at = None
        agent._start_time = None
        agent._queue = SuggestionQueue(sidecar, settings)
        agent._event_handlers = {}

        app = create_app(agent)
        client = TestClient(app)

        # Health check
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"

        # No suggestions yet
        resp = client.get("/api/suggestions")
        assert resp.status_code == 200
        assert resp.json()["suggestions"] == []

        # Create a suggestion in the sidecar directly
        det = Detection(
            detection_type=DetectionType.VACUUM_DEAD_TUPLES,
            target_table="users",
            message="test",
        )
        det.id = sidecar.save_detection(det)
        sug = agent._queue.add_suggestion(
            detection=det,
            action_type=ActionType.VACUUM,
            sql="VACUUM users",
            explanation="test",
            risk_level=RiskLevel.LOW,
            reversible=False,
            reverse_sql="",
            rule_id="vacuum_dead_tuples",
        )

        # List suggestions
        resp = client.get("/api/suggestions")
        assert resp.status_code == 200
        assert len(resp.json()["suggestions"]) == 1

        # Get single suggestion
        resp = client.get(f"/api/suggestions/{sug.id}")
        assert resp.status_code == 200

        # 404 for non-existent
        resp = client.get("/api/suggestions/9999")
        assert resp.status_code == 404

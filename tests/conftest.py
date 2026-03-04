"""Shared test fixtures for pgAgent."""

import pytest

from pgagent.config import Settings
from pgagent.sidecar import SidecarDB


@pytest.fixture
def settings():
    """Test settings with defaults."""
    return Settings(pg_dsn="postgresql://test:test@localhost:5432/test")


@pytest.fixture
def sidecar():
    """In-memory sidecar database."""
    db = SidecarDB(":memory:")
    yield db
    db.close()

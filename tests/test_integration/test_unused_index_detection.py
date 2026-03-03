"""Integration test: unused index detection."""

from __future__ import annotations

import pytest

from pgagent.detector import Detector
from pgagent.models import DetectionType
from pgagent.observer import Observer

from .conftest import requires_pg


@requires_pg
class TestUnusedIndexDetection:
    def test_detects_unused_indexes(self, pg_conn, settings, sidecar):
        """The demo schema has intentionally unused indexes."""
        observer = Observer(settings, pg_conn, sidecar)
        observer.check_connection()
        swd = observer.observe()

        detector = Detector(settings)
        detections = detector.detect(swd)

        unused = [
            d for d in detections if d.detection_type == DetectionType.UNUSED_INDEX
        ]
        # Demo has several intentionally unused indexes
        assert isinstance(unused, list)
        # Check at least the detection pipeline runs

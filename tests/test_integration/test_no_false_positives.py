"""Integration test: verify no false positives on a healthy database."""

from __future__ import annotations

import pytest

from pgagent.detector import Detector
from pgagent.models import DetectionType, Severity
from pgagent.observer import Observer

from .conftest import requires_pg


@requires_pg
class TestNoFalsePositives:
    def test_no_critical_on_fresh_snapshot(self, pg_conn, settings, sidecar):
        """A fresh snapshot should not produce CRITICAL detections under normal load."""
        observer = Observer(settings, pg_conn, sidecar)
        observer.check_connection()
        swd = observer.observe()

        detector = Detector(settings)
        detections = detector.detect(swd)

        # No CRITICAL severity should fire under demo normal conditions
        critical = [d for d in detections if d.severity == Severity.CRITICAL]
        assert len(critical) == 0, f"Unexpected critical detections: {critical}"

    def test_connection_saturation_not_triggered(self, pg_conn, settings, sidecar):
        """With few connections, saturation should not fire."""
        observer = Observer(settings, pg_conn, sidecar)
        observer.check_connection()
        swd = observer.observe()

        detector = Detector(settings)
        detections = detector.detect(swd)

        saturation = [
            d
            for d in detections
            if d.detection_type == DetectionType.CONNECTION_SATURATION
        ]
        assert len(saturation) == 0

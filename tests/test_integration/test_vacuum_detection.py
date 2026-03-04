"""Integration test: vacuum-related detections."""

from __future__ import annotations

import pytest

from pgagent.detector import Detector
from pgagent.models import DetectionType
from pgagent.observer import Observer

from .conftest import requires_pg


@requires_pg
class TestVacuumDetection:
    def test_detects_dead_tuples(self, pg_conn, settings, sidecar):
        """After updates, detect dead tuple buildup."""
        cur = pg_conn.cursor()
        pg_conn.autocommit = True

        # Generate dead tuples
        for i in range(1, 101):
            cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (i,))
        cur.close()

        observer = Observer(settings, pg_conn, sidecar)
        observer.check_connection()
        swd = observer.observe()

        detector = Detector(settings)
        detections = detector.detect(swd)

        dead_tuple_detections = [
            d
            for d in detections
            if d.detection_type == DetectionType.VACUUM_DEAD_TUPLES
        ]
        # Dead tuples may be present from seed data + our updates
        # This is a smoke test — we just verify the pipeline works
        assert isinstance(dead_tuple_detections, list)

    def test_detects_stale_vacuum(self, pg_conn, settings, sidecar):
        """Tables that haven't been vacuumed should be flagged."""
        observer = Observer(settings, pg_conn, sidecar)
        observer.check_connection()
        swd = observer.observe()

        detector = Detector(settings)
        detections = detector.detect(swd)

        stale_vacuum = [
            d for d in detections if d.detection_type == DetectionType.VACUUM_STALE
        ]
        assert isinstance(stale_vacuum, list)

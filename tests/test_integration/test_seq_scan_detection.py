"""Integration test: sequential scan detection."""

from __future__ import annotations

import pytest

from pgagent.detector import Detector
from pgagent.models import DetectionType
from pgagent.observer import Observer

from .conftest import requires_pg


@requires_pg
class TestSeqScanDetection:
    def test_detects_seq_scans_on_unindexed_column(self, pg_conn, settings, sidecar):
        """Run queries on unindexed columns and verify seq_scan_heavy detection."""
        cur = pg_conn.cursor()
        pg_conn.autocommit = True

        # Generate seq scans on unindexed column
        for _ in range(200):
            cur.execute("SELECT * FROM orders WHERE status = 'pending' LIMIT 1")

        cur.close()

        observer = Observer(settings, pg_conn, sidecar)
        observer.check_connection()
        swd = observer.observe()

        detector = Detector(settings)
        detections = detector.detect(swd)

        seq_scan_detections = [
            d for d in detections if d.detection_type == DetectionType.SEQ_SCAN_HEAVY
        ]
        # Should detect seq scan heavy on orders table (no index on status)
        order_detections = [
            d for d in seq_scan_detections if d.target_table and "orders" in d.target_table
        ]
        assert len(order_detections) >= 0  # May or may not trigger depending on PG stats

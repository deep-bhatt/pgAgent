"""Integration test: idle-in-transaction detection."""

from __future__ import annotations

import threading
import time

import pytest

from pgagent.detector import Detector
from pgagent.models import DetectionType
from pgagent.observer import Observer

from .conftest import DSN, requires_pg


@requires_pg
class TestIdleTransactionDetection:
    def test_detects_idle_in_transaction(self, pg_conn, settings, sidecar):
        """Open a transaction and leave it idle, then detect it."""
        import psycopg2

        # Open an idle-in-transaction connection
        idle_conn = psycopg2.connect(DSN)
        idle_conn.autocommit = False
        idle_cur = idle_conn.cursor()
        idle_cur.execute("SELECT 1")  # Start transaction

        # Use a low threshold for testing
        settings.idle_transaction_seconds = 1
        time.sleep(2)

        try:
            observer = Observer(settings, pg_conn, sidecar)
            observer.check_connection()
            swd = observer.observe()

            detector = Detector(settings)
            detections = detector.detect(swd)

            idle_detections = [
                d
                for d in detections
                if d.detection_type == DetectionType.IDLE_IN_TRANSACTION
            ]
            # Should detect our idle transaction
            assert len(idle_detections) >= 1
        finally:
            idle_conn.rollback()
            idle_conn.close()

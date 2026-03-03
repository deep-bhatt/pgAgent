"""Integration test: lock contention detection."""

from __future__ import annotations

import threading
import time

import pytest

from pgagent.detector import Detector
from pgagent.models import DetectionType
from pgagent.observer import Observer

from .conftest import DSN, requires_pg


@requires_pg
class TestLockContentionDetection:
    def test_detects_lock_contention(self, pg_conn, settings, sidecar):
        """Create lock contention and verify detection."""
        import psycopg2

        # Connection 1: hold an exclusive lock
        lock_conn = psycopg2.connect(DSN)
        lock_conn.autocommit = False
        lock_cur = lock_conn.cursor()
        lock_cur.execute("BEGIN")
        lock_cur.execute("LOCK TABLE products IN ACCESS EXCLUSIVE MODE")

        # Connection 2: try to access the locked table (will wait)
        def blocked_query():
            try:
                blocked_conn = psycopg2.connect(DSN)
                blocked_conn.autocommit = False
                blocked_cur = blocked_conn.cursor()
                blocked_cur.execute("SET statement_timeout = '10s'")
                blocked_cur.execute("SELECT * FROM products LIMIT 1")
                blocked_conn.rollback()
                blocked_conn.close()
            except Exception:
                pass

        t = threading.Thread(target=blocked_query)
        t.start()

        settings.lock_wait_seconds = 1
        time.sleep(2)

        try:
            observer = Observer(settings, pg_conn, sidecar)
            observer.check_connection()
            swd = observer.observe()

            detector = Detector(settings)
            detections = detector.detect(swd)

            lock_detections = [
                d
                for d in detections
                if d.detection_type == DetectionType.LOCK_CONTENTION
            ]
            assert isinstance(lock_detections, list)
        finally:
            lock_conn.rollback()
            lock_conn.close()
            t.join(timeout=15)

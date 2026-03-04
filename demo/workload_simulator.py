#!/usr/bin/env python3
"""Workload simulator for pgAgent demo — generates detectable problems."""

import argparse
import logging
import os
import random
import sys
import time
import threading

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("workload_simulator")

DSN = os.environ.get("PGAGENT_PG_DSN", "postgresql://pgagent:pgagent@localhost:5433/demo")


def get_conn(autocommit: bool = True) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(DSN)
    if autocommit:
        conn.autocommit = True
    return conn


# ── Scenario: Sequential scans on unindexed columns ──────────────────────


def scenario_seq_scans(duration: int) -> None:
    """Generate sequential scans by querying unindexed columns."""
    logger.info("Starting seq_scans scenario for %ds", duration)
    conn = get_conn()
    cur = conn.cursor()
    end = time.time() + duration
    queries = [
        "SELECT * FROM orders WHERE user_id = %s",
        "SELECT * FROM orders WHERE status = %s",
        "SELECT * FROM order_items WHERE order_id = %s",
        "SELECT * FROM users WHERE email = %s",
        "SELECT * FROM products WHERE category = %s",
    ]
    params = [
        lambda: (random.randint(1, 100000),),
        lambda: (random.choice(["pending", "processing", "shipped"]),),
        lambda: (random.randint(1, 200000),),
        lambda: (f"user_{random.randint(1, 100000)}@example.com",),
        lambda: (random.choice(["electronics", "clothing", "books"]),),
    ]
    count = 0
    while time.time() < end:
        idx = random.randint(0, len(queries) - 1)
        try:
            cur.execute(queries[idx], params[idx]())
            cur.fetchall()
            count += 1
        except Exception as e:
            logger.warning("seq_scan query error: %s", e)
        time.sleep(0.01)
    cur.close()
    conn.close()
    logger.info("seq_scans: executed %d queries", count)


# ── Scenario: Dead tuple buildup ─────────────────────────────────────────


def scenario_dead_tuples(duration: int) -> None:
    """Generate dead tuples by updating rows repeatedly without vacuuming."""
    logger.info("Starting dead_tuples scenario for %ds", duration)
    conn = get_conn()
    cur = conn.cursor()
    end = time.time() + duration
    count = 0
    while time.time() < end:
        user_id = random.randint(1, 100000)
        try:
            cur.execute(
                "UPDATE users SET last_login = NOW() WHERE id = %s", (user_id,)
            )
            count += 1
        except Exception as e:
            logger.warning("dead_tuple update error: %s", e)
        time.sleep(0.005)
    cur.close()
    conn.close()
    logger.info("dead_tuples: updated %d rows", count)


# ── Scenario: Idle-in-transaction connections ─────────────────────────────


def scenario_idle_transactions(duration: int) -> None:
    """Open transactions and leave them idle."""
    logger.info("Starting idle_transactions scenario for %ds", duration)
    conns = []
    for i in range(5):
        try:
            conn = get_conn(autocommit=False)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            # Leave transaction open (idle in transaction)
            conns.append(conn)
            logger.info("Opened idle transaction #%d", i + 1)
        except Exception as e:
            logger.warning("idle_transaction error: %s", e)

    time.sleep(min(duration, 600))

    for conn in conns:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
    logger.info("idle_transactions: closed %d connections", len(conns))


# ── Scenario: Lock contention ────────────────────────────────────────────


def scenario_lock_contention(duration: int) -> None:
    """Create lock contention by holding exclusive locks."""
    logger.info("Starting lock_contention scenario for %ds", duration)

    def hold_lock(lock_duration: int) -> None:
        try:
            conn = get_conn(autocommit=False)
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute("LOCK TABLE products IN ACCESS EXCLUSIVE MODE")
            logger.info("Lock acquired, holding for %ds", lock_duration)
            time.sleep(lock_duration)
            conn.rollback()
            conn.close()
        except Exception as e:
            logger.warning("lock_holder error: %s", e)

    def attempt_access(duration: int) -> None:
        end = time.time() + duration
        while time.time() < end:
            try:
                conn = get_conn(autocommit=False)
                cur = conn.cursor()
                cur.execute("SET statement_timeout = '5s'")
                cur.execute("SELECT * FROM products WHERE id = 1 FOR UPDATE")
                conn.rollback()
                conn.close()
            except Exception:
                pass
            time.sleep(1)

    lock_time = min(duration, 60)
    t1 = threading.Thread(target=hold_lock, args=(lock_time,))
    t2 = threading.Thread(target=attempt_access, args=(lock_time,))
    t1.start()
    time.sleep(1)  # Let lock acquire first
    t2.start()
    t1.join()
    t2.join()
    logger.info("lock_contention: done")


# ── Scenario: Connection saturation ──────────────────────────────────────


def scenario_connection_saturation(duration: int) -> None:
    """Open many connections to approach max_connections."""
    logger.info("Starting connection_saturation scenario for %ds", duration)
    conns = []
    # Open up to 80 connections (out of default 100)
    for i in range(80):
        try:
            conn = get_conn()
            conns.append(conn)
        except Exception as e:
            logger.info("connection_saturation: stopped at %d connections: %s", i, e)
            break

    logger.info("Opened %d connections, holding for %ds", len(conns), min(duration, 120))
    time.sleep(min(duration, 120))

    for conn in conns:
        try:
            conn.close()
        except Exception:
            pass
    logger.info("connection_saturation: closed %d connections", len(conns))


# ── Run all scenarios ─────────────────────────────────────────────────────

SCENARIOS = {
    "seq_scans": scenario_seq_scans,
    "dead_tuples": scenario_dead_tuples,
    "idle_transactions": scenario_idle_transactions,
    "lock_contention": scenario_lock_contention,
    "connection_saturation": scenario_connection_saturation,
}


def run_scenario(name: str, duration: int) -> None:
    if name == "all":
        threads = []
        for sname, sfunc in SCENARIOS.items():
            t = threading.Thread(target=sfunc, args=(duration,), name=sname)
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
    elif name in SCENARIOS:
        SCENARIOS[name](duration)
    else:
        logger.error("Unknown scenario: %s. Available: %s", name, ", ".join(SCENARIOS))
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="pgAgent workload simulator")
    parser.add_argument(
        "--scenario",
        default="all",
        choices=list(SCENARIOS.keys()) + ["all"],
        help="Scenario to run (default: all)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN (default: env PGAGENT_PG_DSN or localhost:5433/demo)",
    )
    args = parser.parse_args()

    if args.dsn:
        global DSN
        DSN = args.dsn

    logger.info("Running scenario=%s duration=%ds dsn=%s", args.scenario, args.duration, DSN)
    run_scenario(args.scenario, args.duration)
    logger.info("All scenarios complete.")


if __name__ == "__main__":
    main()

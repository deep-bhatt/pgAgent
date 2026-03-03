# pgAgent

**Autonomous PostgreSQL monitoring and optimization agent.**

pgAgent continuously observes your PostgreSQL database through system views, detects performance problems with deterministic rules, uses an LLM for index recommendations, and presents actionable suggestions through a real-time dashboard. Approved actions are executed safely with automatic rollback if performance degrades.

## How It Works

```
Observe ──> Detect ──> Reason ──> Suggest ──> Execute ──> Evaluate
  │            │          │          │           │           │
  │  pg_stat   │  11      │  Groq    │  Queue    │  Safety   │  Auto-
  │  views     │  rules   │  LLM     │  + UI     │  checks   │  rollback
  └────────────┴──────────┴──────────┴───────────┴───────────┴──────────
                    ↻ repeats every N seconds
```

1. **Observe** — Collects snapshots from `pg_stat_user_tables`, `pg_stat_activity`, `pg_locks`, `pg_stat_bgwriter`, `pg_stat_statements`, and more
2. **Detect** — Runs 11 deterministic rules (vacuum needs, unused indexes, seq scan heavy tables, idle transactions, lock contention, connection saturation, table bloat, checkpoint pressure)
3. **Reason** — Routes complex detections (e.g. seq-scan-heavy queries) to an LLM (Groq/Llama 3.3-70b) for index recommendations
4. **Suggest** — Queues deduplicated suggestions with severity, risk level, and expiry. Low-risk actions can be auto-approved
5. **Execute** — Runs approved SQL through a safety validator (7 checks: no system catalog mods, CONCURRENTLY enforcement, rate limits, concurrent mutation limits)
6. **Evaluate** — Compares pre/post metrics after a configurable delay. Automatically rolls back reversible actions that degrade performance

## Features

- 11 built-in detection rules with configurable thresholds
- LLM-powered index recommendations with graceful degradation
- Suggestion deduplication, cooldowns, and TTL expiry
- 7 safety validation checks before any SQL execution
- Automatic rollback for degraded actions
- Real-time dashboard with WebSocket updates
- SQLite sidecar for state persistence (zero external dependencies)
- Comprehensive REST API

## Architecture

```
pgAgent/
├── backend/              Python agent + API server
│   └── pgagent/
│       ├── agent.py            Main loop (APScheduler)
│       ├── observer.py         PostgreSQL snapshot collection
│       ├── rules.py            11 deterministic detection rules
│       ├── detector.py         Rule aggregation + LLM routing
│       ├── reasoner.py         Groq LLM integration
│       ├── suggestion_queue.py Lifecycle management + dedup
│       ├── validator.py        7 safety checks
│       ├── executor.py         SQL execution with pre-snapshots
│       ├── evaluator.py        Outcome assessment + auto-rollback
│       ├── sidecar.py          SQLite persistence
│       ├── api/                FastAPI REST + WebSocket
│       └── tools/              SQL action implementations
├── frontend/             React dashboard
│   └── src/
│       ├── pages/              Overview, Suggestions, History, Queries, Settings
│       ├── components/         HealthGauge, SuggestionCard, MetricChart, etc.
│       ├── hooks/              TanStack Query hooks + WebSocket
│       └── api/                Typed API client
├── demo/                 Docker-based demo environment
└── tests/                Unit + integration tests
```

## Quick Start

### Prerequisites

- Python 3.12+
- Node.js 18+
- PostgreSQL 13+ (or use the included demo)
- [Groq API key](https://console.groq.com/) (optional — agent works without LLM, just skips index recommendations)

### 1. Start a Demo Database (optional)

If you don't have a PostgreSQL instance to monitor, use the included demo environment:

```bash
cd demo
docker compose up -d
```

This starts PostgreSQL 16 on port **5433** with `pg_stat_statements` enabled, sample tables (users, orders, products, order_items), and seed data.

### 2. Install & Run the Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Copy the example env file and edit it:

```bash
cp .env.example .env
# Edit .env — at minimum set PGAGENT_PG_DSN and optionally PGAGENT_GROQ_API_KEY
```

The defaults in `.env.example` point at the demo database (`localhost:5433/demo`). If you're using the demo, you can start right away:

```bash
pgagent
```

The agent starts on **http://localhost:8420** by default.

### 3. Install & Run the Frontend

```bash
cd frontend
npm install
npm run dev
```

Open **http://localhost:5173** in your browser. The Vite dev server proxies API requests to the backend automatically.

### 4. Generate Some Problems (optional)

Use the workload simulator to create detectable issues:

```bash
cd demo
python workload_simulator.py --scenario all --duration 60
```

Available scenarios: `seq_scans`, `dead_tuples`, `idle_transactions`, `lock_contention`, `connection_saturation`, `all`

## Dashboard Pages

### Overview
Health gauge, connection pool utilization, agent status, time-series charts for connections and sequential scans, active lock chains, and per-table health cards.

### Suggestions
Tabbed view of pending, approved, executed, and rejected suggestions. Each card shows the detection rule, target table, risk level, suggested SQL, and reasoning. Approve or reject with one click.

### History
Audit log of all executed actions with outcome classification (improved, no change, degraded, rolled back, failed). Expandable rows show the SQL that was run and pre/post metric snapshots.

### Query Analysis
Top queries by total execution time from `pg_stat_statements`. Highlights index opportunities — queries with high call counts and slow mean execution times that may benefit from indexes.

### Settings
Configure observation interval, detection thresholds, auto-approval policy, LLM model and timeout. Pause/resume the agent loop.

## Configuration

All settings use environment variables with the `PGAGENT_` prefix. Key options:

| Variable | Default | Description |
|----------|---------|-------------|
| `PGAGENT_PG_DSN` | `postgresql://localhost:5432/postgres` | PostgreSQL connection string |
| `PGAGENT_GROQ_API_KEY` | *(empty)* | Groq API key for LLM features |
| `PGAGENT_LLM_MODEL` | `llama-3.3-70b-versatile` | LLM model for index recommendations |
| `PGAGENT_OBSERVE_INTERVAL_SECONDS` | `60` | Seconds between observation cycles |
| `PGAGENT_DEAD_TUPLE_RATIO_THRESHOLD` | `0.05` | Dead tuple ratio to trigger vacuum suggestion |
| `PGAGENT_SEQ_SCAN_RATIO_THRESHOLD` | `0.5` | Seq scan ratio to flag tables |
| `PGAGENT_IDLE_TRANSACTION_SECONDS` | `300` | Idle-in-transaction timeout |
| `PGAGENT_CONNECTION_SATURATION_RATIO` | `0.8` | Connection pool usage alert threshold |
| `PGAGENT_AUTO_APPROVE_LOW_RISK` | `false` | Auto-approve low-risk suggestions |
| `PGAGENT_SUGGESTION_TTL_SECONDS` | `3600` | Suggestion expiry time |
| `PGAGENT_MAX_INDEX_CREATES_PER_HOUR` | `3` | Rate limit for index creation |
| `PGAGENT_MAX_CONCURRENT_MUTATIONS` | `1` | Max concurrent DDL operations |
| `PGAGENT_API_HOST` | `0.0.0.0` | API server bind address |
| `PGAGENT_API_PORT` | `8420` | API server port |
| `PGAGENT_SIDECAR_DB_PATH` | `pgagent_sidecar.db` | SQLite database file path |

All settings can also be changed at runtime through the Settings page or `PUT /api/config`.

## API Reference

### Health & Status
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Agent health + PostgreSQL connection status |
| GET | `/api/health/history` | Historical snapshots with metrics |
| GET | `/api/status` | Agent state, cycle count, pending suggestions |
| POST | `/api/agent/pause` | Pause the observation loop |
| POST | `/api/agent/resume` | Resume the observation loop |

### Suggestions
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/suggestions` | List suggestions (filterable by `?status=pending`) |
| GET | `/api/suggestions/{id}` | Get suggestion details |
| POST | `/api/suggestions/{id}/approve` | Approve a suggestion for execution |
| POST | `/api/suggestions/{id}/reject` | Reject a suggestion |

### Actions
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/actions` | List executed actions |
| GET | `/api/actions/{id}` | Get action details with pre/post snapshots |

### Queries
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/queries` | Query stats from pg_stat_statements |

### Configuration
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/config` | Current configuration |
| PUT | `/api/config` | Update configuration (partial updates supported) |

### WebSocket
| Endpoint | Description |
|----------|-------------|
| `ws://host/ws/metrics` | Real-time metric snapshots |
| `ws://host/ws/suggestions` | New/updated suggestions |
| `ws://host/ws/actions` | Action execution events |

## Detection Rules

| Rule | Detects | Suggested Action |
|------|---------|-----------------|
| `VACUUM_DEAD_TUPLES` | Dead tuple ratio exceeds threshold | VACUUM |
| `VACUUM_STALE` | No vacuum in configured hours | VACUUM |
| `ANALYZE_STALE` | No analyze in configured hours | ANALYZE |
| `UNUSED_INDEX` | Index unused for N days above size threshold | DROP INDEX |
| `SEQ_SCAN_HEAVY` | High seq scan ratio on table (routes to LLM) | CREATE INDEX |
| `IDLE_IN_TRANSACTION` | Connection idle in transaction too long | KILL CONNECTION |
| `LOCK_CONTENTION` | Lock wait exceeds threshold | Advisory (informational) |
| `CONNECTION_SATURATION` | Connection pool near capacity | Advisory |
| `TABLE_BLOAT` | Estimated bloat ratio exceeds threshold | VACUUM FULL (advisory) |
| `HIGH_BACKEND_WRITES` | Backend write pressure elevated | Advisory |
| `FORCED_CHECKPOINTS` | Checkpoint request ratio high | Advisory |

## Safety Checks

Before executing any suggestion, the validator enforces:

1. **System catalog protection** — Rejects actions targeting `pg_catalog` or `information_schema`
2. **Primary key / unique index protection** — Prevents dropping PK or unique constraint indexes
3. **CONCURRENTLY enforcement** — Index create/drop must use `CONCURRENTLY` to avoid table locks
4. **Index operation rate limits** — Max N creates/drops per hour
5. **Kill threshold** — Limits connections terminated per cycle
6. **Rollback cooldown** — Waits after a rollback before retrying similar actions
7. **Concurrent mutation limit** — Only one DDL mutation at a time

## Running Tests

```bash
cd backend
source .venv/bin/activate

# Unit tests (no PostgreSQL required)
pytest tests/test_unit/ -v

# Integration tests (requires demo PostgreSQL running)
cd ../demo && docker compose up -d && cd ../backend
pytest tests/test_integration/ -v

# All tests
pytest -v
```

## Production Build

```bash
# Build the frontend
cd frontend
npm run build

# The built files are in frontend/dist/
# Serve them with any static file server, or configure the backend to serve them
```

## Tech Stack

**Backend:** Python 3.12+, FastAPI, APScheduler, psycopg2, Pydantic, Groq SDK, SQLite

**Frontend:** React 19, TypeScript, Vite, TanStack Query, Recharts, Tailwind CSS v4, React Router

## License

MIT — see [LICENSE](LICENSE).

# Resume Bullet Points — pgAgent

## Recommended Bullet Points (Full-Stack Focus)

- **Built pgAgent, an autonomous full-stack PostgreSQL monitoring and optimization platform** using Python/FastAPI on the backend and React 19/TypeScript on the frontend, featuring an agent loop that continuously observes database metrics, applies 11 deterministic detection rules, and surfaces actionable suggestions through a real-time WebSocket-powered dashboard.

- **Designed and implemented a 6-stage agentic pipeline** (Observe → Detect → Reason → Suggest → Execute → Evaluate) with 7 pre-execution safety checks including rate limiting, concurrent mutation guards, system catalog protection, and automatic rollback on performance degradation — ensuring safe, human-in-the-loop database optimization.

- **Integrated Groq LLM (Llama 3.3-70b) for intelligent index recommendations** by gathering table schemas, existing indexes, and slow queries from pg_stat_statements, then routing seq-scan-heavy detections to the model with a circuit-breaker pattern (3-failure threshold) for graceful degradation when the LLM is unavailable.

- **Developed a real-time React dashboard with 5 pages** (Overview, Suggestions, History, Query Analysis, Settings) using TanStack Query for server state management, WebSocket channels for live metric/suggestion/action updates, Recharts for time-series visualization, and Tailwind CSS v4 for a responsive dark-themed UI.

- **Engineered a comprehensive test suite and Docker-based demo environment** with pytest unit tests covering all 11 detection rules, 7 safety validators, and the suggestion lifecycle state machine, plus integration tests against a seeded 810K-row PostgreSQL 16 database with a multi-scenario workload simulator for reproducing real-world performance issues.

---

## One-Liner Version (Google Docs — Calibri 11, 0.4" margins)

- Built pgAgent, an autonomous PostgreSQL optimization agent with Python/FastAPI, React 19, and WebSockets
- Designed a 6-stage agentic pipeline with 7 safety checks, mutation guards, rate limiting, and auto-rollback
- Integrated Groq LLM (Llama 3.3-70b) for index recommendations on slow queries with circuit-breaker fallback
- Developed real-time React 19 dashboard with TanStack Query, WebSocket channels, Recharts, and Tailwind v4
- Engineered comprehensive pytest suite for 11 detection rules and 7 validators with 810K-row PostgreSQL demo

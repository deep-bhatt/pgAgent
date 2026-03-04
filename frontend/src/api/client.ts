const BASE = "";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<T>;
}

// Health
export const getHealth = () => request<HealthResponse>("/api/health");
export const getHealthHistory = () => request<HealthHistoryResponse>("/api/health/history");
export const getStatus = () => request<StatusResponse>("/api/status");
export const pauseAgent = () => request<{ status: string }>("/api/agent/pause", { method: "POST" });
export const resumeAgent = () => request<{ status: string }>("/api/agent/resume", { method: "POST" });

// Suggestions
export const getSuggestions = (status?: string) =>
  request<SuggestionsResponse>(`/api/suggestions${status ? `?status=${status}` : ""}`);
export const getSuggestion = (id: number) =>
  request<{ suggestion: Suggestion }>(`/api/suggestions/${id}`);
export const approveSuggestion = (id: number) =>
  request<{ suggestion: Suggestion }>(`/api/suggestions/${id}/approve`, { method: "POST" });
export const rejectSuggestion = (id: number) =>
  request<{ suggestion: Suggestion }>(`/api/suggestions/${id}/reject`, { method: "POST" });

// Actions
export const getActions = (outcome?: string) =>
  request<ActionsResponse>(`/api/actions${outcome ? `?outcome=${outcome}` : ""}`);
export const getAction = (id: number) =>
  request<{ action: Action }>(`/api/actions/${id}`);

// Queries
export const getQueries = () => request<QueriesResponse>("/api/queries");

// Config
export const getConfig = () => request<Record<string, unknown>>("/api/config");
export const updateConfig = (data: Record<string, unknown>) =>
  request<ConfigUpdateResponse>("/api/config", { method: "PUT", body: JSON.stringify(data) });

// Types

export interface HealthResponse {
  status: string;
  agent_running: boolean;
  pg_connected: boolean;
  uptime_seconds: number;
}

export interface StatusResponse {
  paused: boolean;
  cycle_count: number;
  last_cycle_at: string;
  detections_24h: number;
  pending_suggestions: number;
}

export interface SnapshotData {
  timestamp: string;
  data: {
    tables?: TableStats[];
    indexes?: IndexStats[];
    connections?: ConnectionStats;
    queries?: QueryStats[];
    bgwriter?: BgwriterStats;
    locks?: LockInfo[];
    pg_version?: number;
    has_pg_stat_statements?: boolean;
  };
}

export interface HealthHistoryResponse {
  snapshots: { id: number; timestamp: string; data: SnapshotData["data"] }[];
}

export interface TableStats {
  schema_name: string;
  table_name: string;
  n_live_tup: number;
  n_dead_tup: number;
  seq_scan: number;
  idx_scan: number;
  last_vacuum: string | null;
  last_autovacuum: string | null;
  last_analyze: string | null;
  table_size_bytes: number;
}

export interface IndexStats {
  schema_name: string;
  table_name: string;
  index_name: string;
  idx_scan: number;
  index_size_bytes: number;
  is_unique: boolean;
  is_primary: boolean;
  index_def: string;
}

export interface ConnectionStats {
  total_connections: number;
  active: number;
  idle: number;
  idle_in_transaction: number;
  waiting: number;
  max_connections: number;
}

export interface QueryStats {
  queryid: number | null;
  query: string;
  calls: number;
  total_exec_time: number;
  mean_exec_time: number;
  rows: number;
  shared_blks_hit: number;
  shared_blks_read: number;
}

export interface BgwriterStats {
  checkpoints_timed: number;
  checkpoints_req: number;
  buffers_backend: number;
  buffers_alloc: number;
}

export interface LockInfo {
  pid: number;
  locktype: string;
  mode: string;
  granted: boolean;
  relation: string | null;
  state: string;
  query: string;
  wait_duration_seconds: number | null;
  blocked_by: number[];
}

export interface Suggestion {
  id: number;
  detection_id: number | null;
  rule_id: string;
  action_type: string;
  target_table: string | null;
  target_index: string | null;
  target_pid: number | null;
  sql: string;
  explanation: string;
  risk_level: string;
  reversible: boolean;
  reverse_sql: string;
  status: string;
  created_at: string;
  expires_at: string | null;
  approved_at: string | null;
  rejected_at: string | null;
}

export interface SuggestionsResponse {
  suggestions: Suggestion[];
}

export interface Action {
  id: number;
  suggestion_id: number;
  action_type: string;
  sql_executed: string;
  target_table: string | null;
  target_index: string | null;
  outcome: string;
  outcome_details: string;
  executed_at: string;
  evaluated_at: string | null;
  rollback_sql: string;
  rolled_back: boolean;
  pre_snapshot: Record<string, unknown>;
  post_snapshot: Record<string, unknown>;
}

export interface ActionsResponse {
  actions: Action[];
}

export interface QueriesResponse {
  queries: QueryStats[];
  has_pg_stat_statements: boolean;
  snapshot_timestamp: string | null;
}

export interface ConfigUpdateResponse {
  updated: string[];
  rejected_protected: string[];
  unknown: string[];
  config: Record<string, unknown>;
}

import { useMemo } from "react";
import { useHealth, useHealthHistory, useStatus } from "@/hooks/useHealth";
import { HealthGauge, computeHealthScore } from "@/components/HealthGauge";
import { TableHealthCard } from "@/components/TableHealthCard";
import { MetricChart } from "@/components/MetricChart";
import { LockChainView } from "@/components/LockChainView";
import { cn, formatDuration } from "@/lib/utils";
import type { TableStats, ConnectionStats, LockInfo } from "@/api/client";
import {
  Activity,
  Database,
  Pause,
  Play,
  Server,
  AlertCircle,
} from "lucide-react";

export default function Overview() {
  const { data: health } = useHealth();
  const { data: history } = useHealthHistory();
  const { data: status } = useStatus();

  const latestSnapshot = useMemo(() => {
    const snaps = history?.snapshots ?? [];
    if (snaps.length === 0) return null;
    return snaps[snaps.length - 1]?.data ?? null;
  }, [history]);

  const tables: TableStats[] = (latestSnapshot?.tables as TableStats[] | undefined) ?? [];
  const connections: ConnectionStats | null =
    (latestSnapshot?.connections as ConnectionStats | undefined) ?? null;
  const locks: LockInfo[] = (latestSnapshot?.locks as LockInfo[] | undefined) ?? [];

  const healthScore = useMemo(
    () => (latestSnapshot ? computeHealthScore(latestSnapshot as Parameters<typeof computeHealthScore>[0]) : 85),
    [latestSnapshot]
  );

  const topTables = useMemo(
    () =>
      [...tables]
        .sort((a, b) => b.table_size_bytes - a.table_size_bytes)
        .slice(0, 10),
    [tables]
  );

  // Chart data from history
  const chartData = useMemo(() => {
    return (history?.snapshots ?? []).map((snap) => {
      const d = snap.data;
      const conns = d?.connections as ConnectionStats | undefined;
      const tbls = (d?.tables ?? []) as TableStats[];
      const totalSeq = tbls.reduce((s, t) => s + (t.seq_scan ?? 0), 0);
      const totalIdx = tbls.reduce((s, t) => s + (t.idx_scan ?? 0), 0);

      return {
        time: new Date(snap.timestamp).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        }),
        connections: conns?.total_connections ?? 0,
        seq_scans: totalSeq,
        idx_scans: totalIdx,
      };
    });
  }, [history]);

  const connRatio =
    connections && connections.max_connections > 0
      ? connections.total_connections / connections.max_connections
      : 0;

  return (
    <div className="space-y-6">
      {/* Top row: health gauge + connection pool + agent status */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Health Score */}
        <div className="rounded-lg border border-border bg-card p-6 flex items-center justify-center">
          <HealthGauge score={healthScore} />
        </div>

        {/* Connection Pool */}
        <div className="rounded-lg border border-border bg-card p-6 space-y-4">
          <h2 className="text-sm font-semibold flex items-center gap-2">
            <Server size={16} /> Connection Pool
          </h2>
          {connections ? (
            <>
              <div className="grid grid-cols-2 gap-3 text-xs">
                <div>
                  <span className="text-muted-foreground">Active</span>
                  <p className="text-lg font-bold text-green-400">
                    {connections.active}
                  </p>
                </div>
                <div>
                  <span className="text-muted-foreground">Idle</span>
                  <p className="text-lg font-bold">{connections.idle}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">Idle in txn</span>
                  <p
                    className={cn(
                      "text-lg font-bold",
                      connections.idle_in_transaction > 0
                        ? "text-orange-400"
                        : ""
                    )}
                  >
                    {connections.idle_in_transaction}
                  </p>
                </div>
                <div>
                  <span className="text-muted-foreground">Total / Max</span>
                  <p className="text-lg font-bold">
                    {connections.total_connections} /{" "}
                    {connections.max_connections}
                  </p>
                </div>
              </div>
              <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
                <div
                  className={cn(
                    "h-full rounded-full transition-all",
                    connRatio > 0.9
                      ? "bg-red-500"
                      : connRatio > 0.8
                        ? "bg-orange-500"
                        : "bg-green-500"
                  )}
                  style={{ width: `${connRatio * 100}%` }}
                />
              </div>
            </>
          ) : (
            <p className="text-sm text-muted-foreground">
              No connection data yet
            </p>
          )}
        </div>

        {/* Agent Status */}
        <div className="rounded-lg border border-border bg-card p-6 space-y-4">
          <h2 className="text-sm font-semibold flex items-center gap-2">
            <Activity size={16} /> Agent Status
          </h2>
          <div className="space-y-3 text-sm">
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">State</span>
              {status?.paused ? (
                <span className="flex items-center gap-1 text-orange-400">
                  <Pause size={14} /> Paused
                </span>
              ) : health?.agent_running ? (
                <span className="flex items-center gap-1 text-green-400">
                  <Play size={14} /> Running
                </span>
              ) : (
                <span className="flex items-center gap-1 text-yellow-400">
                  <AlertCircle size={14} /> Not connected
                </span>
              )}
            </div>
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">PostgreSQL</span>
              <span
                className={cn(
                  health?.pg_connected ? "text-green-400" : "text-red-400"
                )}
              >
                {health?.pg_connected ? "Connected" : "Disconnected"}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">Cycles</span>
              <span>{status?.cycle_count ?? 0}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">Uptime</span>
              <span>
                {health?.uptime_seconds
                  ? formatDuration(health.uptime_seconds)
                  : "—"}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">Detections (24h)</span>
              <span className="font-medium">
                {status?.detections_24h ?? 0}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">Pending</span>
              {(status?.pending_suggestions ?? 0) > 0 ? (
                <span className="px-2 py-0.5 rounded-full bg-primary/20 text-primary text-xs font-medium">
                  {status?.pending_suggestions}
                </span>
              ) : (
                <span>0</span>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Charts */}
      {chartData.length > 1 && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="rounded-lg border border-border bg-card p-4">
            <MetricChart
              data={chartData}
              dataKey="connections"
              label="Connections"
              color="#3b82f6"
              height={180}
            />
          </div>
          <div className="rounded-lg border border-border bg-card p-4">
            <MetricChart
              data={chartData}
              dataKey="seq_scans"
              label="Sequential Scans"
              color="#f59e0b"
              height={180}
            />
          </div>
        </div>
      )}

      {/* Active Locks */}
      <div className="rounded-lg border border-border bg-card p-4">
        <h2 className="text-sm font-semibold mb-3 flex items-center gap-2">
          <AlertCircle size={16} /> Active Locks
        </h2>
        <LockChainView locks={locks} />
      </div>

      {/* Table Health Grid */}
      {topTables.length > 0 && (
        <div>
          <h2 className="text-sm font-semibold mb-3 flex items-center gap-2">
            <Database size={16} /> Table Health
          </h2>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-3">
            {topTables.map((t) => (
              <TableHealthCard key={`${t.schema_name}.${t.table_name}`} table={t} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

import type { TableStats } from "@/api/client";
import { cn, formatNumber, formatBytes, timeAgo } from "@/lib/utils";

interface TableHealthCardProps {
  table: TableStats;
}

export function TableHealthCard({ table }: TableHealthCardProps) {
  const total = table.n_live_tup + table.n_dead_tup;
  const deadRatio = total > 0 ? table.n_dead_tup / total : 0;
  const totalScans = table.seq_scan + table.idx_scan;
  const seqRatio = totalScans > 0 ? table.seq_scan / totalScans : 0;

  const deadColor =
    deadRatio > 0.2
      ? "text-red-400"
      : deadRatio > 0.05
        ? "text-yellow-400"
        : "text-green-400";

  const scanColor =
    seqRatio > 0.5 && totalScans > 100
      ? "text-orange-400"
      : "text-green-400";

  const lastVacuum = table.last_vacuum ?? table.last_autovacuum;

  return (
    <div className="rounded-lg border border-border bg-card p-4 space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="font-semibold text-sm truncate" title={table.table_name}>
          {table.table_name}
        </h3>
        <span className="text-xs text-muted-foreground">
          {formatBytes(table.table_size_bytes)}
        </span>
      </div>

      <div className="grid grid-cols-2 gap-2 text-xs">
        <div>
          <span className="text-muted-foreground">Rows</span>
          <p className="font-medium">{formatNumber(table.n_live_tup)}</p>
        </div>
        <div>
          <span className="text-muted-foreground">Dead tuples</span>
          <p className={cn("font-medium", deadColor)}>
            {(deadRatio * 100).toFixed(1)}%
          </p>
        </div>
        <div>
          <span className="text-muted-foreground">Seq / Idx scans</span>
          <p className={cn("font-medium", scanColor)}>
            {formatNumber(table.seq_scan)} / {formatNumber(table.idx_scan)}
          </p>
        </div>
        <div>
          <span className="text-muted-foreground">Last vacuum</span>
          <p className="font-medium">
            {lastVacuum ? timeAgo(lastVacuum) : "never"}
          </p>
        </div>
      </div>

      {/* Dead tuple ratio bar */}
      <div className="h-1.5 w-full rounded-full bg-secondary overflow-hidden">
        <div
          className={cn(
            "h-full rounded-full transition-all",
            deadRatio > 0.2
              ? "bg-red-500"
              : deadRatio > 0.05
                ? "bg-yellow-500"
                : "bg-green-500"
          )}
          style={{ width: `${Math.min(deadRatio * 100 * 5, 100)}%` }}
        />
      </div>
    </div>
  );
}

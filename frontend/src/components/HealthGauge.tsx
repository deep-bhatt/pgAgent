import { cn } from "@/lib/utils";

interface HealthGaugeProps {
  score: number;
  label?: string;
}

export function HealthGauge({ score, label = "Health Score" }: HealthGaugeProps) {
  const clampedScore = Math.max(0, Math.min(100, Math.round(score)));
  const radius = 80;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (clampedScore / 100) * circumference;

  const color =
    clampedScore >= 80
      ? "text-green-400 stroke-green-400"
      : clampedScore >= 60
        ? "text-yellow-400 stroke-yellow-400"
        : clampedScore >= 40
          ? "text-orange-400 stroke-orange-400"
          : "text-red-400 stroke-red-400";

  return (
    <div className="flex flex-col items-center">
      <div className="relative w-48 h-48">
        <svg className="w-48 h-48 -rotate-90" viewBox="0 0 200 200">
          <circle
            cx="100"
            cy="100"
            r={radius}
            fill="none"
            className="stroke-secondary"
            strokeWidth="12"
          />
          <circle
            cx="100"
            cy="100"
            r={radius}
            fill="none"
            className={cn("transition-all duration-700", color)}
            strokeWidth="12"
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
          />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span className={cn("text-4xl font-bold", color.split(" ")[0])}>
            {clampedScore}
          </span>
          <span className="text-sm text-muted-foreground">{label}</span>
        </div>
      </div>
    </div>
  );
}

export function computeHealthScore(snapshot: {
  tables?: Array<{ n_live_tup: number; n_dead_tup: number; seq_scan: number; idx_scan: number }>;
  connections?: { total_connections: number; max_connections: number; idle_in_transaction: number };
  locks?: Array<{ granted: boolean }>;
}): number {
  let score = 100;

  // Dead tuple penalty
  const tables = snapshot.tables ?? [];
  for (const t of tables) {
    const total = t.n_live_tup + t.n_dead_tup;
    if (total > 0) {
      const ratio = t.n_dead_tup / total;
      if (ratio > 0.1) score -= 5;
      if (ratio > 0.2) score -= 5;
    }
  }

  // Seq scan ratio penalty
  for (const t of tables) {
    const totalScans = t.seq_scan + t.idx_scan;
    if (totalScans > 100) {
      const seqRatio = t.seq_scan / totalScans;
      if (seqRatio > 0.5) score -= 3;
    }
  }

  // Connection saturation
  const conns = snapshot.connections;
  if (conns && conns.max_connections > 0) {
    const ratio = conns.total_connections / conns.max_connections;
    if (ratio > 0.8) score -= 15;
    else if (ratio > 0.6) score -= 5;
    if (conns.idle_in_transaction > 3) score -= 5;
  }

  // Lock contention
  const locks = snapshot.locks ?? [];
  const blocked = locks.filter((l) => !l.granted).length;
  if (blocked > 0) score -= blocked * 5;

  return Math.max(0, Math.min(100, score));
}

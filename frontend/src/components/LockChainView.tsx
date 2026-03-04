import type { LockInfo } from "@/api/client";
import { cn, formatDuration } from "@/lib/utils";
import { AlertTriangle } from "lucide-react";

interface LockChainViewProps {
  locks: LockInfo[];
}

export function LockChainView({ locks }: LockChainViewProps) {
  const blocked = locks.filter((l) => !l.granted);

  if (blocked.length === 0) {
    return (
      <div className="text-sm text-muted-foreground flex items-center gap-2 py-4">
        <span className="text-green-400">No active lock contention</span>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {blocked.map((lock, i) => (
        <div
          key={`${lock.pid}-${i}`}
          className="flex items-start gap-3 rounded-lg border border-orange-500/20 bg-orange-500/5 p-3"
        >
          <AlertTriangle size={16} className="text-orange-400 mt-0.5 shrink-0" />
          <div className="flex-1 min-w-0 text-xs">
            <div className="flex items-center gap-2 flex-wrap">
              {lock.blocked_by.length > 0 && (
                <>
                  <span className="font-mono text-red-400">
                    PID {lock.blocked_by.join(", ")}
                  </span>
                  <span className="text-muted-foreground">&rarr;</span>
                </>
              )}
              <span className="font-mono text-orange-300">
                PID {lock.pid}
              </span>
              <span
                className={cn(
                  "px-1.5 py-0.5 rounded text-xs",
                  "bg-orange-500/10 text-orange-400"
                )}
              >
                {lock.mode}
              </span>
              {lock.wait_duration_seconds != null && (
                <span className="text-muted-foreground">
                  waiting {formatDuration(lock.wait_duration_seconds)}
                </span>
              )}
            </div>
            {lock.relation && (
              <p className="text-muted-foreground mt-1">
                on <span className="font-mono text-foreground">{lock.relation}</span>
              </p>
            )}
            {lock.query && (
              <p className="mt-1 text-muted-foreground truncate" title={lock.query}>
                {lock.query.slice(0, 120)}
              </p>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

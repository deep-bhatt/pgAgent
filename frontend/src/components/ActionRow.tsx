import { useState } from "react";
import type { Action } from "@/api/client";
import { cn, outcomeColor, timeAgo } from "@/lib/utils";
import { ChevronDown, ChevronUp } from "lucide-react";

interface ActionRowProps {
  action: Action;
}

export function ActionRow({ action }: ActionRowProps) {
  const [expanded, setExpanded] = useState(false);

  const outColor = outcomeColor[action.outcome] ?? "text-gray-400 bg-gray-400/10";

  return (
    <div className="border-b border-border last:border-b-0">
      <button
        className="w-full p-4 text-left flex items-center gap-4 hover:bg-secondary/20 transition-colors text-sm"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="text-xs text-muted-foreground w-28 shrink-0">
          {timeAgo(action.executed_at)}
        </span>

        <span className="font-mono text-xs w-28 shrink-0">
          {action.action_type.replace(/_/g, " ")}
        </span>

        <span className="flex-1 truncate text-muted-foreground">
          {action.target_table ?? action.target_index ?? "—"}
        </span>

        <span
          className={cn(
            "text-xs px-2 py-0.5 rounded-full font-medium",
            outColor
          )}
        >
          {action.outcome.replace(/_/g, " ")}
        </span>

        {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
      </button>

      {expanded && (
        <div className="px-4 pb-4 space-y-3 bg-background/50">
          <div>
            <h4 className="text-xs font-semibold text-muted-foreground mb-1">
              SQL Executed
            </h4>
            <pre className="text-xs bg-secondary/50 rounded p-2 overflow-x-auto font-mono">
              {action.sql_executed}
            </pre>
          </div>

          {action.outcome_details && (
            <div>
              <h4 className="text-xs font-semibold text-muted-foreground mb-1">
                Outcome Details
              </h4>
              <p className="text-xs text-muted-foreground">
                {action.outcome_details}
              </p>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div>
              <h4 className="text-xs font-semibold text-muted-foreground mb-1">
                Pre-Action Snapshot
              </h4>
              <pre className="text-xs bg-secondary/50 rounded p-2 overflow-x-auto font-mono">
                {JSON.stringify(action.pre_snapshot, null, 2)}
              </pre>
            </div>
            <div>
              <h4 className="text-xs font-semibold text-muted-foreground mb-1">
                Post-Action Snapshot
              </h4>
              <pre className="text-xs bg-secondary/50 rounded p-2 overflow-x-auto font-mono">
                {JSON.stringify(action.post_snapshot, null, 2)}
              </pre>
            </div>
          </div>

          {action.rolled_back && action.rollback_sql && (
            <div>
              <h4 className="text-xs font-semibold text-muted-foreground mb-1">
                Rollback SQL (executed)
              </h4>
              <pre className="text-xs bg-orange-500/10 rounded p-2 overflow-x-auto font-mono text-orange-300">
                {action.rollback_sql}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

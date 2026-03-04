import { useState } from "react";
import type { Suggestion } from "@/api/client";
import { cn, severityColor, timeAgo } from "@/lib/utils";
import {
  Database,
  Search,
  Trash2,
  Zap,
  Clock,
  AlertTriangle,
  ChevronDown,
  ChevronUp,
  Check,
  X,
} from "lucide-react";

interface SuggestionCardProps {
  suggestion: Suggestion;
  onApprove?: (id: number) => void;
  onReject?: (id: number) => void;
  isApproving?: boolean;
  isRejecting?: boolean;
}

const categoryIcon: Record<string, React.ReactNode> = {
  vacuum: <Trash2 size={16} />,
  vacuum_dead_tuples: <Trash2 size={16} />,
  vacuum_stale: <Clock size={16} />,
  analyze_stale: <Search size={16} />,
  unused_index: <Database size={16} />,
  seq_scan_heavy: <Search size={16} />,
  idle_in_transaction: <AlertTriangle size={16} />,
  create_index: <Database size={16} />,
  drop_index: <Database size={16} />,
  kill_connection: <Zap size={16} />,
};

const riskBadge: Record<string, string> = {
  low: "bg-green-500/10 text-green-400 border-green-500/30",
  medium: "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  high: "bg-red-500/10 text-red-400 border-red-500/30",
};

export function SuggestionCard({
  suggestion,
  onApprove,
  onReject,
  isApproving,
  isRejecting,
}: SuggestionCardProps) {
  const [expanded, setExpanded] = useState(false);
  const severity = suggestion.rule_id.includes("dead_tuples")
    ? "high"
    : suggestion.risk_level;

  const sevColor = severityColor[severity] ?? severityColor["medium"];

  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      {/* Header */}
      <button
        className="w-full p-4 text-left flex items-start gap-3 hover:bg-secondary/30 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className={cn("p-2 rounded-md", sevColor)}>
          {categoryIcon[suggestion.rule_id] ??
            categoryIcon[suggestion.action_type] ?? <Database size={16} />}
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-medium text-sm">
              {suggestion.action_type.replace(/_/g, " ").toUpperCase()}
            </span>
            <span className={cn("text-xs px-2 py-0.5 rounded-full border", sevColor)}>
              {severity}
            </span>
            <span
              className={cn(
                "text-xs px-2 py-0.5 rounded-full border",
                riskBadge[suggestion.risk_level] ?? riskBadge["medium"]
              )}
            >
              risk: {suggestion.risk_level}
            </span>
          </div>
          <p className="text-sm text-muted-foreground mt-1 truncate">
            {suggestion.target_table && (
              <span className="text-foreground font-mono text-xs mr-2">
                {suggestion.target_table}
              </span>
            )}
            {suggestion.explanation}
          </p>
          <span className="text-xs text-muted-foreground">
            {timeAgo(suggestion.created_at)}
            {suggestion.expires_at && (
              <> &middot; expires {timeAgo(suggestion.expires_at)}</>
            )}
          </span>
        </div>

        {expanded ? (
          <ChevronUp size={16} className="text-muted-foreground mt-1" />
        ) : (
          <ChevronDown size={16} className="text-muted-foreground mt-1" />
        )}
      </button>

      {/* Expanded detail */}
      {expanded && (
        <div className="border-t border-border p-4 space-y-3 bg-background/50">
          <div>
            <h4 className="text-xs font-semibold text-muted-foreground mb-1">
              Explanation
            </h4>
            <p className="text-sm">{suggestion.explanation}</p>
          </div>
          <div>
            <h4 className="text-xs font-semibold text-muted-foreground mb-1">
              SQL to execute
            </h4>
            <pre className="text-xs bg-secondary/50 rounded p-2 overflow-x-auto font-mono">
              {suggestion.sql}
            </pre>
          </div>
          {suggestion.reverse_sql && (
            <div>
              <h4 className="text-xs font-semibold text-muted-foreground mb-1">
                Rollback SQL
              </h4>
              <pre className="text-xs bg-secondary/50 rounded p-2 overflow-x-auto font-mono">
                {suggestion.reverse_sql}
              </pre>
            </div>
          )}

          {/* Actions */}
          {suggestion.status === "pending" && (
            <div className="flex gap-2 pt-2">
              <button
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-green-600 hover:bg-green-700 text-white text-sm font-medium disabled:opacity-50 transition-colors"
                onClick={() => onApprove?.(suggestion.id)}
                disabled={isApproving}
              >
                <Check size={14} />
                {isApproving ? "Approving…" : "Approve"}
              </button>
              <button
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-red-600 hover:bg-red-700 text-white text-sm font-medium disabled:opacity-50 transition-colors"
                onClick={() => onReject?.(suggestion.id)}
                disabled={isRejecting}
              >
                <X size={14} />
                {isRejecting ? "Rejecting…" : "Reject"}
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

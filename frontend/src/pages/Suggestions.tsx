import { useState } from "react";
import {
  useSuggestions,
  useApproveSuggestion,
  useRejectSuggestion,
} from "@/hooks/useSuggestions";
import { SuggestionCard } from "@/components/SuggestionCard";
import { cn } from "@/lib/utils";

const TABS = [
  { label: "Pending", value: "pending" },
  { label: "Approved", value: "approved" },
  { label: "Executed", value: "executed" },
  { label: "Rejected", value: "rejected" },
  { label: "All", value: undefined },
] as const;

export default function Suggestions() {
  const [activeTab, setActiveTab] = useState<string | undefined>("pending");
  const { data, isLoading } = useSuggestions(activeTab);
  const approve = useApproveSuggestion();
  const reject = useRejectSuggestion();

  const suggestions = data?.suggestions ?? [];

  // Sort by severity proxy (rule_id and risk level)
  const sorted = [...suggestions].sort((a, b) => {
    const severityOrder: Record<string, number> = {
      critical: 0,
      high: 1,
      medium: 2,
      low: 3,
    };
    return (
      (severityOrder[a.risk_level] ?? 2) - (severityOrder[b.risk_level] ?? 2)
    );
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold">Suggestions</h1>
        <span className="text-sm text-muted-foreground">
          {suggestions.length} suggestion{suggestions.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 bg-secondary/30 rounded-lg p-1">
        {TABS.map((tab) => (
          <button
            key={tab.label}
            className={cn(
              "px-3 py-1.5 rounded-md text-sm font-medium transition-colors",
              activeTab === tab.value
                ? "bg-primary text-white"
                : "text-muted-foreground hover:text-foreground"
            )}
            onClick={() => setActiveTab(tab.value)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Suggestion list */}
      {isLoading ? (
        <div className="text-center py-12 text-muted-foreground">
          Loading suggestions…
        </div>
      ) : sorted.length === 0 ? (
        <div className="text-center py-12 text-muted-foreground">
          No {activeTab ?? ""} suggestions
        </div>
      ) : (
        <div className="space-y-3">
          {sorted.map((sug) => (
            <SuggestionCard
              key={sug.id}
              suggestion={sug}
              onApprove={(id) => approve.mutate(id)}
              onReject={(id) => reject.mutate(id)}
              isApproving={
                approve.isPending &&
                approve.variables === sug.id
              }
              isRejecting={
                reject.isPending &&
                reject.variables === sug.id
              }
            />
          ))}
        </div>
      )}

      {/* Bulk approve for pending low-risk */}
      {activeTab === "pending" && sorted.filter((s) => s.risk_level === "low").length > 1 && (
        <div className="flex justify-end">
          <button
            className="px-4 py-2 rounded-md bg-green-600/20 text-green-400 border border-green-600/30 text-sm font-medium hover:bg-green-600/30 transition-colors"
            onClick={() => {
              sorted
                .filter((s) => s.risk_level === "low" && s.status === "pending")
                .forEach((s) => approve.mutate(s.id));
            }}
          >
            Approve all low-risk ({sorted.filter((s) => s.risk_level === "low" && s.status === "pending").length})
          </button>
        </div>
      )}
    </div>
  );
}

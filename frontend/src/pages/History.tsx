import { useState } from "react";
import { useActions } from "@/hooks/useActions";
import { ActionRow } from "@/components/ActionRow";
import { cn } from "@/lib/utils";

const OUTCOME_FILTERS = [
  { label: "All", value: undefined },
  { label: "Pending eval", value: "pending_evaluation" },
  { label: "Improved", value: "improved" },
  { label: "Success", value: "success" },
  { label: "No change", value: "no_change" },
  { label: "Degraded", value: "degraded" },
  { label: "Rolled back", value: "rolled_back" },
  { label: "Failed", value: "failed" },
] as const;

export default function History() {
  const [outcomeFilter, setOutcomeFilter] = useState<string | undefined>(
    undefined
  );
  const { data, isLoading } = useActions(outcomeFilter);

  const actions = data?.actions ?? [];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold">Action History</h1>
        <span className="text-sm text-muted-foreground">
          {actions.length} action{actions.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Outcome filter chips */}
      <div className="flex gap-1 flex-wrap">
        {OUTCOME_FILTERS.map((f) => (
          <button
            key={f.label}
            className={cn(
              "px-3 py-1 rounded-full text-xs font-medium transition-colors border",
              outcomeFilter === f.value
                ? "bg-primary/20 text-primary border-primary/40"
                : "text-muted-foreground border-border hover:border-muted-foreground/30"
            )}
            onClick={() => setOutcomeFilter(f.value)}
          >
            {f.label}
          </button>
        ))}
      </div>

      {/* Action list */}
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        {/* Header */}
        <div className="grid grid-cols-12 gap-4 px-4 py-2 bg-secondary/30 text-xs font-medium text-muted-foreground">
          <div className="col-span-2">Time</div>
          <div className="col-span-2">Action</div>
          <div className="col-span-5">Target</div>
          <div className="col-span-2">Outcome</div>
          <div className="col-span-1" />
        </div>

        {isLoading ? (
          <div className="text-center py-12 text-muted-foreground text-sm">
            Loading actions…
          </div>
        ) : actions.length === 0 ? (
          <div className="text-center py-12 text-muted-foreground text-sm">
            No actions found
          </div>
        ) : (
          actions.map((action) => (
            <ActionRow key={action.id} action={action} />
          ))
        )}
      </div>
    </div>
  );
}

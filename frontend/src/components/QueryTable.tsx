import { useState } from "react";
import type { QueryStats } from "@/api/client";
import { cn, formatNumber } from "@/lib/utils";
import { ChevronDown, ChevronUp, ArrowUpDown } from "lucide-react";

interface QueryTableProps {
  queries: QueryStats[];
}

type SortKey = "total_exec_time" | "mean_exec_time" | "calls" | "rows";

export function QueryTable({ queries }: QueryTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("total_exec_time");
  const [sortDesc, setSortDesc] = useState(true);
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);

  const sorted = [...queries].sort((a, b) => {
    const av = a[sortKey];
    const bv = b[sortKey];
    return sortDesc ? bv - av : av - bv;
  });

  function handleSort(key: SortKey) {
    if (key === sortKey) {
      setSortDesc(!sortDesc);
    } else {
      setSortKey(key);
      setSortDesc(true);
    }
  }

  const SortHeader = ({ label, field }: { label: string; field: SortKey }) => (
    <button
      className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
      onClick={() => handleSort(field)}
    >
      {label}
      {sortKey === field ? (
        sortDesc ? (
          <ChevronDown size={12} />
        ) : (
          <ChevronUp size={12} />
        )
      ) : (
        <ArrowUpDown size={12} className="opacity-40" />
      )}
    </button>
  );

  return (
    <div className="border border-border rounded-lg overflow-hidden">
      {/* Header */}
      <div className="grid grid-cols-12 gap-2 px-4 py-2 bg-secondary/30 text-xs font-medium">
        <div className="col-span-5 text-muted-foreground">Query</div>
        <div className="col-span-2">
          <SortHeader label="Total time" field="total_exec_time" />
        </div>
        <div className="col-span-2">
          <SortHeader label="Mean time" field="mean_exec_time" />
        </div>
        <div className="col-span-1">
          <SortHeader label="Calls" field="calls" />
        </div>
        <div className="col-span-1">
          <SortHeader label="Rows" field="rows" />
        </div>
        <div className="col-span-1 text-muted-foreground">Cache</div>
      </div>

      {/* Rows */}
      {sorted.length === 0 ? (
        <div className="px-4 py-8 text-center text-sm text-muted-foreground">
          No query data available
        </div>
      ) : (
        sorted.map((q, i) => {
          const cacheHits = q.shared_blks_hit + q.shared_blks_read;
          const cacheRatio =
            cacheHits > 0 ? (q.shared_blks_hit / cacheHits) * 100 : 0;

          return (
            <div key={q.queryid ?? i} className="border-t border-border">
              <button
                className="w-full grid grid-cols-12 gap-2 px-4 py-2 text-xs hover:bg-secondary/20 transition-colors text-left"
                onClick={() =>
                  setExpandedIdx(expandedIdx === i ? null : i)
                }
              >
                <div className="col-span-5 font-mono truncate text-muted-foreground">
                  {q.query.slice(0, 80)}
                </div>
                <div className="col-span-2">
                  {(q.total_exec_time / 1000).toFixed(2)}s
                </div>
                <div className="col-span-2">
                  {q.mean_exec_time.toFixed(2)}ms
                </div>
                <div className="col-span-1">{formatNumber(q.calls)}</div>
                <div className="col-span-1">{formatNumber(q.rows)}</div>
                <div className="col-span-1">
                  <span
                    className={cn(
                      cacheRatio > 95
                        ? "text-green-400"
                        : cacheRatio > 80
                          ? "text-yellow-400"
                          : "text-red-400"
                    )}
                  >
                    {cacheRatio.toFixed(0)}%
                  </span>
                </div>
              </button>

              {expandedIdx === i && (
                <div className="px-4 pb-3 bg-background/50">
                  <pre className="text-xs bg-secondary/50 rounded p-2 overflow-x-auto font-mono whitespace-pre-wrap">
                    {q.query}
                  </pre>
                </div>
              )}
            </div>
          );
        })
      )}
    </div>
  );
}

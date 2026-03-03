import { useQuery } from "@tanstack/react-query";
import { getQueries } from "@/api/client";
import { QueryTable } from "@/components/QueryTable";
import { AlertCircle, Search } from "lucide-react";

export default function Queries() {
  const { data, isLoading } = useQuery({
    queryKey: ["queries"],
    queryFn: getQueries,
    refetchInterval: 30_000,
  });

  const hasPgStatStatements = data?.has_pg_stat_statements ?? false;
  const queries = data?.queries ?? [];

  if (!hasPgStatStatements && !isLoading) {
    return (
      <div className="space-y-6">
        <h1 className="text-xl font-bold">Query Analysis</h1>
        <div className="rounded-lg border border-border bg-card p-12 text-center space-y-3">
          <AlertCircle
            size={48}
            className="mx-auto text-muted-foreground"
          />
          <h2 className="text-lg font-semibold">
            pg_stat_statements not available
          </h2>
          <p className="text-sm text-muted-foreground max-w-md mx-auto">
            Query analysis requires the pg_stat_statements extension. Enable it
            in your PostgreSQL configuration:
          </p>
          <pre className="text-xs bg-secondary/50 rounded p-3 inline-block text-left font-mono">
            {`shared_preload_libraries = 'pg_stat_statements'\npg_stat_statements.track = all`}
          </pre>
        </div>
      </div>
    );
  }

  // Find queries that hit unindexed tables (high seq scan, low idx scan in the query)
  const indexOpportunities = queries.filter(
    (q) => q.calls > 10 && q.mean_exec_time > 50
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold">Query Analysis</h1>
        {data?.snapshot_timestamp && (
          <span className="text-xs text-muted-foreground">
            Last snapshot:{" "}
            {new Date(data.snapshot_timestamp).toLocaleTimeString()}
          </span>
        )}
      </div>

      {/* Slow Queries */}
      <div>
        <h2 className="text-sm font-semibold mb-3 flex items-center gap-2">
          <Search size={16} /> Slow Queries (by total execution time)
        </h2>
        {isLoading ? (
          <div className="text-center py-12 text-muted-foreground text-sm">
            Loading query data…
          </div>
        ) : (
          <QueryTable queries={queries} />
        )}
      </div>

      {/* Index Opportunities */}
      {indexOpportunities.length > 0 && (
        <div className="rounded-lg border border-yellow-500/20 bg-yellow-500/5 p-4">
          <h2 className="text-sm font-semibold mb-2 text-yellow-400">
            Index Opportunities
          </h2>
          <p className="text-xs text-muted-foreground mb-3">
            Queries with high call count and mean execution time &gt; 50ms may
            benefit from indexes.
          </p>
          <div className="space-y-2">
            {indexOpportunities.slice(0, 5).map((q, i) => (
              <div
                key={i}
                className="text-xs font-mono text-muted-foreground truncate"
                title={q.query}
              >
                <span className="text-yellow-400 mr-2">
                  {q.mean_exec_time.toFixed(1)}ms
                </span>
                <span className="mr-2">({q.calls} calls)</span>
                {q.query.slice(0, 100)}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getConfig,
  updateConfig,
  pauseAgent,
  resumeAgent,
} from "@/api/client";
import { useStatus } from "@/hooks/useHealth";
import { cn } from "@/lib/utils";
import { Save, Pause, Play, RefreshCw } from "lucide-react";

interface SettingFieldProps {
  label: string;
  description?: string;
  children: React.ReactNode;
}

function SettingField({ label, description, children }: SettingFieldProps) {
  return (
    <div className="flex items-start justify-between gap-8 py-4 border-b border-border last:border-b-0">
      <div>
        <label className="text-sm font-medium">{label}</label>
        {description && (
          <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
        )}
      </div>
      <div className="shrink-0">{children}</div>
    </div>
  );
}

export default function Settings() {
  const qc = useQueryClient();
  const { data: config, isLoading } = useQuery({
    queryKey: ["config"],
    queryFn: getConfig,
  });
  const { data: status } = useStatus();

  const [form, setForm] = useState<Record<string, unknown>>({});
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    if (config) setForm(config);
  }, [config]);

  const saveMutation = useMutation({
    mutationFn: (data: Record<string, unknown>) => updateConfig(data),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["config"] });
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    },
  });

  const pauseMutation = useMutation({
    mutationFn: pauseAgent,
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["status"] }),
  });

  const resumeMutation = useMutation({
    mutationFn: resumeAgent,
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["status"] }),
  });

  function handleSave() {
    // Only send changed fields
    const changed: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(form)) {
      if (config && config[key] !== val) {
        changed[key] = val;
      }
    }
    if (Object.keys(changed).length > 0) {
      saveMutation.mutate(changed);
    }
  }

  function setField(key: string, value: unknown) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  if (isLoading) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        Loading configuration…
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-2xl">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold">Settings</h1>
        <button
          className={cn(
            "flex items-center gap-1.5 px-4 py-2 rounded-md text-sm font-medium transition-colors",
            saved
              ? "bg-green-600 text-white"
              : "bg-primary text-white hover:bg-primary/90"
          )}
          onClick={handleSave}
          disabled={saveMutation.isPending}
        >
          {saved ? (
            <>
              <Save size={14} /> Saved
            </>
          ) : saveMutation.isPending ? (
            <>
              <RefreshCw size={14} className="animate-spin" /> Saving…
            </>
          ) : (
            <>
              <Save size={14} /> Save Changes
            </>
          )}
        </button>
      </div>

      {/* Agent Control */}
      <div className="rounded-lg border border-border bg-card p-4">
        <h2 className="text-sm font-semibold mb-3">Agent Control</h2>
        <SettingField label="Agent Mode" description="Pause or resume the observation loop">
          <button
            className={cn(
              "flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors",
              status?.paused
                ? "bg-green-600 hover:bg-green-700 text-white"
                : "bg-orange-600 hover:bg-orange-700 text-white"
            )}
            onClick={() =>
              status?.paused
                ? resumeMutation.mutate()
                : pauseMutation.mutate()
            }
            disabled={pauseMutation.isPending || resumeMutation.isPending}
          >
            {status?.paused ? (
              <>
                <Play size={14} /> Resume
              </>
            ) : (
              <>
                <Pause size={14} /> Pause
              </>
            )}
          </button>
        </SettingField>
      </div>

      {/* Observation Settings */}
      <div className="rounded-lg border border-border bg-card p-4">
        <h2 className="text-sm font-semibold mb-3">Observation</h2>
        <SettingField
          label="Observation Interval"
          description="Seconds between observation cycles (10–300)"
        >
          <input
            type="number"
            min={10}
            max={300}
            value={Number(form.observe_interval_seconds ?? 60)}
            onChange={(e) =>
              setField("observe_interval_seconds", Number(e.target.value))
            }
            className="w-24 rounded-md bg-secondary border border-border px-2 py-1 text-sm"
          />
        </SettingField>
        <SettingField
          label="Auto-approve Low Risk"
          description="Automatically approve suggestions with low risk level"
        >
          <button
            className={cn(
              "w-12 h-6 rounded-full transition-colors relative",
              form.auto_approve_low_risk ? "bg-green-600" : "bg-secondary"
            )}
            onClick={() =>
              setField("auto_approve_low_risk", !form.auto_approve_low_risk)
            }
          >
            <div
              className={cn(
                "w-5 h-5 rounded-full bg-white absolute top-0.5 transition-transform",
                form.auto_approve_low_risk ? "translate-x-6" : "translate-x-0.5"
              )}
            />
          </button>
        </SettingField>
      </div>

      {/* Detection Thresholds */}
      <div className="rounded-lg border border-border bg-card p-4">
        <h2 className="text-sm font-semibold mb-3">Detection Thresholds</h2>
        <SettingField
          label="Dead Tuple Ratio"
          description="Threshold for dead tuple detection (0.0–1.0)"
        >
          <input
            type="number"
            step={0.01}
            min={0}
            max={1}
            value={Number(form.dead_tuple_ratio_threshold ?? 0.05)}
            onChange={(e) =>
              setField("dead_tuple_ratio_threshold", Number(e.target.value))
            }
            className="w-24 rounded-md bg-secondary border border-border px-2 py-1 text-sm"
          />
        </SettingField>
        <SettingField
          label="Idle Transaction Threshold"
          description="Seconds before idle-in-transaction connections are flagged"
        >
          <input
            type="number"
            min={10}
            value={Number(form.idle_transaction_seconds ?? 300)}
            onChange={(e) =>
              setField("idle_transaction_seconds", Number(e.target.value))
            }
            className="w-24 rounded-md bg-secondary border border-border px-2 py-1 text-sm"
          />
        </SettingField>
        <SettingField
          label="Seq Scan Ratio"
          description="Threshold for seq scan heavy detection (0.0–1.0)"
        >
          <input
            type="number"
            step={0.1}
            min={0}
            max={1}
            value={Number(form.seq_scan_ratio_threshold ?? 0.5)}
            onChange={(e) =>
              setField("seq_scan_ratio_threshold", Number(e.target.value))
            }
            className="w-24 rounded-md bg-secondary border border-border px-2 py-1 text-sm"
          />
        </SettingField>
        <SettingField
          label="Connection Saturation"
          description="Ratio of connections to max before alerting (0.0–1.0)"
        >
          <input
            type="number"
            step={0.05}
            min={0}
            max={1}
            value={Number(form.connection_saturation_ratio ?? 0.8)}
            onChange={(e) =>
              setField("connection_saturation_ratio", Number(e.target.value))
            }
            className="w-24 rounded-md bg-secondary border border-border px-2 py-1 text-sm"
          />
        </SettingField>
      </div>

      {/* LLM Settings */}
      <div className="rounded-lg border border-border bg-card p-4">
        <h2 className="text-sm font-semibold mb-3">LLM Configuration</h2>
        <SettingField label="Model" description="LLM model for index recommendations">
          <input
            type="text"
            value={String(form.llm_model ?? "")}
            onChange={(e) => setField("llm_model", e.target.value)}
            className="w-56 rounded-md bg-secondary border border-border px-2 py-1 text-sm font-mono"
          />
        </SettingField>
        <SettingField
          label="Timeout"
          description="LLM request timeout in seconds"
        >
          <input
            type="number"
            min={5}
            max={120}
            value={Number(form.llm_timeout_seconds ?? 30)}
            onChange={(e) =>
              setField("llm_timeout_seconds", Number(e.target.value))
            }
            className="w-24 rounded-md bg-secondary border border-border px-2 py-1 text-sm"
          />
        </SettingField>
      </div>
    </div>
  );
}

import { useQuery } from "@tanstack/react-query";
import { getHealth, getHealthHistory, getStatus } from "@/api/client";

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: getHealth,
    refetchInterval: 30_000,
  });
}

export function useHealthHistory() {
  return useQuery({
    queryKey: ["health-history"],
    queryFn: getHealthHistory,
    refetchInterval: 30_000,
  });
}

export function useStatus() {
  return useQuery({
    queryKey: ["status"],
    queryFn: getStatus,
    refetchInterval: 15_000,
  });
}

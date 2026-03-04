import { useQuery } from "@tanstack/react-query";
import { getActions, getAction } from "@/api/client";

export function useActions(outcome?: string) {
  return useQuery({
    queryKey: ["actions", outcome],
    queryFn: () => getActions(outcome),
    refetchInterval: 15_000,
  });
}

export function useAction(id: number) {
  return useQuery({
    queryKey: ["action", id],
    queryFn: () => getAction(id),
    enabled: id > 0,
  });
}

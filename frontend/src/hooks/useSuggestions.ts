import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getSuggestions,
  getSuggestion,
  approveSuggestion,
  rejectSuggestion,
} from "@/api/client";

export function useSuggestions(status?: string) {
  return useQuery({
    queryKey: ["suggestions", status],
    queryFn: () => getSuggestions(status),
    refetchInterval: 10_000,
  });
}

export function useSuggestion(id: number) {
  return useQuery({
    queryKey: ["suggestion", id],
    queryFn: () => getSuggestion(id),
    enabled: id > 0,
  });
}

export function useApproveSuggestion() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: approveSuggestion,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["suggestions"] });
    },
  });
}

export function useRejectSuggestion() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: rejectSuggestion,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["suggestions"] });
    },
  });
}

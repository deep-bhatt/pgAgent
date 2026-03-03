import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { WebSocketManager } from "@/api/websocket";

export function useWebSocket() {
  const queryClient = useQueryClient();
  const managerRef = useRef<WebSocketManager | null>(null);

  useEffect(() => {
    const mgr = new WebSocketManager(queryClient);
    managerRef.current = mgr;

    mgr.connect("metrics");
    mgr.connect("suggestions");
    mgr.connect("actions");

    return () => {
      mgr.disconnectAll();
    };
  }, [queryClient]);

  return managerRef;
}

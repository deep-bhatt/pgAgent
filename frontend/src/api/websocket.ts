import type { QueryClient } from "@tanstack/react-query";

type Channel = "metrics" | "suggestions" | "actions";

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;

export class WebSocketManager {
  private sockets: Map<Channel, WebSocket> = new Map();
  private reconnectTimers: Map<Channel, ReturnType<typeof setTimeout>> = new Map();
  private reconnectAttempts: Map<Channel, number> = new Map();
  private queryClient: QueryClient;
  private baseUrl: string;

  constructor(queryClient: QueryClient) {
    this.queryClient = queryClient;
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    this.baseUrl = `${proto}//${window.location.host}`;
  }

  connect(channel: Channel): void {
    if (this.sockets.has(channel)) return;

    const url = `${this.baseUrl}/ws/${channel}`;
    const ws = new WebSocket(url);

    ws.onopen = () => {
      this.reconnectAttempts.set(channel, 0);
    };

    ws.onmessage = (event) => {
      try {
        JSON.parse(event.data);
      } catch {
        return;
      }
      this.handleMessage(channel);
    };

    ws.onclose = () => {
      this.sockets.delete(channel);
      this.scheduleReconnect(channel);
    };

    ws.onerror = () => {
      ws.close();
    };

    this.sockets.set(channel, ws);
  }

  disconnect(channel: Channel): void {
    const ws = this.sockets.get(channel);
    if (ws) {
      ws.close();
      this.sockets.delete(channel);
    }
    const timer = this.reconnectTimers.get(channel);
    if (timer) {
      clearTimeout(timer);
      this.reconnectTimers.delete(channel);
    }
  }

  disconnectAll(): void {
    for (const channel of ["metrics", "suggestions", "actions"] as Channel[]) {
      this.disconnect(channel);
    }
  }

  private handleMessage(channel: Channel): void {
    switch (channel) {
      case "metrics":
        void this.queryClient.invalidateQueries({ queryKey: ["health"] });
        void this.queryClient.invalidateQueries({ queryKey: ["health-history"] });
        void this.queryClient.invalidateQueries({ queryKey: ["status"] });
        break;
      case "suggestions":
        void this.queryClient.invalidateQueries({ queryKey: ["suggestions"] });
        break;
      case "actions":
        void this.queryClient.invalidateQueries({ queryKey: ["actions"] });
        break;
    }
  }

  private scheduleReconnect(channel: Channel): void {
    const attempts = this.reconnectAttempts.get(channel) ?? 0;
    const delay = Math.min(RECONNECT_BASE_MS * 2 ** attempts, RECONNECT_MAX_MS);
    this.reconnectAttempts.set(channel, attempts + 1);

    const timer = setTimeout(() => {
      this.reconnectTimers.delete(channel);
      this.connect(channel);
    }, delay);

    this.reconnectTimers.set(channel, timer);
  }
}

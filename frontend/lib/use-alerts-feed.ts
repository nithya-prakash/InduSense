"use client";

import { useEffect, useRef, useState } from "react";
import { wsUrl } from "./api";
import type { AlertEventMessage } from "./types";

/**
 * Subscribes to /ws/alerts and returns the live feed of alert events for
 * the current user's organization (enforced server-side, not filtered here
 * — see docs on ws_alerts.go). Reconnects with backoff on disconnect since
 * a dashboard tab is typically left open far longer than any one WebSocket
 * connection reliably survives (proxy timeouts, laptop sleep, etc).
 */
export function useAlertsFeed() {
  const [events, setEvents] = useState<AlertEventMessage[]>([]);
  const [connected, setConnected] = useState(false);
  const retryDelay = useRef(1000);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let closedByEffect = false;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;

    function connect() {
      ws = new WebSocket(wsUrl("/ws/alerts"));

      ws.onopen = () => {
        setConnected(true);
        retryDelay.current = 1000;
      };
      ws.onmessage = (evt) => {
        try {
          const parsed: AlertEventMessage = JSON.parse(evt.data);
          setEvents((prev) => [parsed, ...prev].slice(0, 50));
        } catch {
          /* ignore malformed frames */
        }
      };
      ws.onclose = () => {
        setConnected(false);
        if (closedByEffect) return;
        retryTimer = setTimeout(connect, retryDelay.current);
        retryDelay.current = Math.min(retryDelay.current * 2, 30000);
      };
      ws.onerror = () => {
        ws?.close();
      };
    }

    connect();
    return () => {
      closedByEffect = true;
      if (retryTimer) clearTimeout(retryTimer);
      ws?.close();
    };
  }, []);

  return { events, connected };
}

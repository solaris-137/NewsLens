import { useCallback, useEffect, useRef, useState } from "react";

import type { WsMessage } from "@/types";

interface UseWebSocketReturn {
  isConnected: boolean;
  lastMessage: WsMessage | null;
}

export function useWebSocket(url: string): UseWebSocketReturn {
  const [isConnected, setIsConnected] = useState(false);
  const [lastMessage, setLastMessage] = useState<WsMessage | null>(null);
  const shouldReconnect = useRef(true);
  const wsRef = useRef<WebSocket | null>(null);
  const retryCount = useRef(0);
  const retryTimer = useRef<ReturnType<typeof setTimeout>>();

  const connect = useCallback(() => {
    wsRef.current = new WebSocket(url);

    wsRef.current.onopen = () => {
      setIsConnected(true);
      retryCount.current = 0;
    };

    wsRef.current.onmessage = (event) => {
      try {
        setLastMessage(JSON.parse(event.data) as WsMessage);
      } catch {
        return;
      }
    };

    wsRef.current.onclose = () => {
      setIsConnected(false);
      if (!shouldReconnect.current) {
        return;
      }
      const delay = Math.min(1000 * 2 ** retryCount.current, 30000);
      retryCount.current += 1;
      retryTimer.current = setTimeout(connect, delay);
    };

    wsRef.current.onerror = () => wsRef.current?.close();
  }, [url]);

  useEffect(() => {
    shouldReconnect.current = true;
    connect();
    return () => {
      shouldReconnect.current = false;
      if (retryTimer.current) {
        clearTimeout(retryTimer.current);
      }
      wsRef.current?.close();
    };
  }, [connect]);

  return { isConnected, lastMessage };
}

import { useEffect, useRef, useState } from 'react';

export interface PositionData {
  symbol: string;
  direction: string;
  quantity: number;
  entry_price: number;
  mark_price: number;
  unrealized_pnl: number;
}

// signal.generated / signal.approved / signal.rejected 事件的统一形态
export interface SignalItem {
  instance?: string;
  symbol: string;
  direction?: string;
  conviction?: number;
  entry_price?: number;
  stop_loss?: number;
  take_profit?: number;
  signal_id?: string;
  strategy?: string;
  timestamp?: number;
  // 风控链决策帧: 'signal.approved' | 'signal.rejected'
  decision?: string;
  reason?: string;
  modifications?: Record<string, unknown>;
}

// order.filled 事件
export interface OrderItem {
  instance?: string;
  symbol: string;
  side?: string;
  order_type?: string;
  status?: string;
  quantity?: number;
  price?: number;
  order_id?: string | number;
  error?: string | null;
}

export interface DashboardData {
  equity: number;
  margin_ratio: number;
  daily_pnl: number;
  drawdown: number;
  position_count: number;
  positions: PositionData[];
  prices: Record<string, { last: number | null; mark: number | null }>;
  signals: SignalItem[];
  orders: OrderItem[];
  heartbeats: Record<string, number>;
}

export interface CommandAck {
  command: string;
  ok: boolean;
  error?: string;
}

export function useWebSocket() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [connected, setConnected] = useState(false);
  const [lastAck, setLastAck] = useState<CommandAck | null>(null);
  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws`;
    ws.current = new WebSocket(url);
    ws.current.onopen = () => setConnected(true);
    ws.current.onclose = () => setConnected(false);
    ws.current.onmessage = (e) => {
      try {
        const frame = JSON.parse(e.data);
        // command_ack 帧 (kill switch 反馈) 不进数据状态, 单独存 lastAck
        if (frame && frame.type === 'command_ack') {
          setLastAck({ command: frame.command, ok: !!frame.ok, error: frame.error });
          return;
        }
        setData(frame);
      } catch {}
    };
    return () => ws.current?.close();
  }, []);

  const send = (msg: string) => ws.current?.send(msg);
  return { data, connected, lastAck, send };
}

import { useEffect, useRef, useState } from 'react';

export interface PositionData {
  symbol: string;
  direction: string;
  quantity: number;
  entry_price: number;
  break_even?: number;
  mark_price: number;
  unrealized_pnl: number;
  leverage?: number;
  liquidation_price?: number | null;
  liq_distance_pct?: number | null;
  adl_quantile?: number | null;
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
  ts?: string;
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
  ts?: string;
}

export interface Ticker {
  symbol: string;
  last: number;
  change_pct: number;
  high: number;
  low: number;
}

export interface DashboardData {
  equity: number;
  margin_ratio: number;
  daily_pnl: number;
  drawdown: number;
  position_count: number;
  positions: PositionData[];
  prices: Record<string, { last: number | null; mark: number | null }>;
  assets: { asset: string; walletBalance: number }[];
  available_balance: number;
  tickers: Ticker[];
  tickers_updated_at: number;
  signals: SignalItem[];
  orders: OrderItem[];
  heartbeats: Record<string, number>;
}

export interface CommandAck {
  command: string;
  ok: boolean;
  error?: string;
}

function storedToken(): string {
  try { return sessionStorage.getItem('dshtoken') || ''; } catch { return ''; }
}

export function useWebSocket() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [connected, setConnected] = useState(false);
  const [lastAck, setLastAck] = useState<CommandAck | null>(null);
  const [needAuth, setNeedAuth] = useState(false);
  const ws = useRef<WebSocket | null>(null);
  const retry = useRef(0);

  useEffect(() => {
    let closed = false;

    const connect = () => {
      if (closed) return;
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const token = storedToken();
      const url = `${proto}//${location.host}/ws${token ? `?token=${encodeURIComponent(token)}` : ''}`;
      const socket = new WebSocket(url);
      ws.current = socket;
      socket.onopen = () => { retry.current = 0; setConnected(true); };
      socket.onclose = (e) => {
        setConnected(false);
        if (closed) return;
        if (e.code === 4401) { setNeedAuth(true); return; }  // 等用户输入 token
        // 自动重连: 指数退避 1s→2s→4s→…→30s (2026-08-16 审计: 原实现断线即死)
        const delay = Math.min(30_000, 1000 * 2 ** retry.current);
        retry.current += 1;
        setTimeout(connect, delay);
      };
      socket.onmessage = (e) => {
        try {
          const frame = JSON.parse(e.data);
          if (frame && frame.type === 'command_ack') {
            setLastAck({ command: frame.command, ok: !!frame.ok, error: frame.error });
            return;
          }
          setData(frame);
        } catch { /* 非 JSON 帧忽略 */ }
      };
    };

    connect();
    return () => { closed = true; ws.current?.close(); };
  }, []);

  const send = (msg: string) => ws.current?.send(msg);

  /** 服务器要求 token 时弹出输入框 (sessionStorage 记忆) */
  const submitToken = (token: string) => {
    try { sessionStorage.setItem('dshtoken', token); } catch { /* 静默 */ }
    setNeedAuth(false);
    retry.current = 0;
    ws.current?.close();
    setTimeout(() => window.location.reload(), 50);
  };

  return { data, connected, lastAck, send, needAuth, submitToken };
}

import { useEffect, useState } from 'react';
import { LineChart, timeLabel } from './charts';
import { KlineChart } from './KlineChart';

/** 交易历史面板: 权益曲线 + K线蜡烛图 + 平仓明细 + 绩效统计 (面板二期 2026-08-16) */

interface EquityPoint {
  ts: number;
  total_equity: number;
  margin_ratio: number;
  daily_pnl: number;
  drawdown: number;
}

interface Trade {
  ts: number;
  symbol: string;
  direction: string;
  entry_price: number;
  exit_price: number;
  quantity: number;
  gross_pnl: number;
  fee: number;
  realized_pnl: number;
}

interface Candle {
  open_time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

async function fetchJson<T>(url: string): Promise<T> {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`${url}: ${resp.status}`);
  return resp.json();
}

function fmtTs(ts: number): string {
  const d = new Date(ts * 1000);
  return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

export function TradingHistory() {
  const [hours, setHours] = useState(24);
  const [equity, setEquity] = useState<EquityPoint[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [candles, setCandles] = useState<Candle[]>([]);
  const [symbol, setSymbol] = useState('BTCUSDT');
  const [timeframe, setTimeframe] = useState('15m');

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const [e, t, k] = await Promise.all([
          fetchJson<{ points: EquityPoint[] }>(`/api/ops/equity?hours=${hours}`),
          fetchJson<{ trades: Trade[] }>('/api/ops/trades?limit=50'),
          fetchJson<{ candles: Candle[] }>(`/api/kline?symbol=${symbol}&timeframe=${timeframe}&limit=500`),
        ]);
        if (!alive) return;
        setEquity(e.points); setTrades(t.trades); setCandles(k.candles);
      } catch { /* 后端未就绪时静默 */ }
    };
    load();
    const timer = setInterval(load, 15_000);
    return () => { alive = false; clearInterval(timer); };
  }, [hours, symbol, timeframe]);

  // 绩效统计 (基于平仓明细)
  const wins = trades.filter(t => t.realized_pnl > 0);
  const losses = trades.filter(t => t.realized_pnl < 0);
  const winRate = trades.length ? (wins.length / trades.length) * 100 : 0;
  const grossWin = wins.reduce((s, t) => s + t.realized_pnl, 0);
  const grossLoss = Math.abs(losses.reduce((s, t) => s + t.realized_pnl, 0));
  const profitFactor = grossLoss > 0 ? grossWin / grossLoss : (grossWin > 0 ? Infinity : 0);
  const avgWin = wins.length ? grossWin / wins.length : 0;
  const avgLoss = losses.length ? grossLoss / losses.length : 0;
  const netPnl = trades.reduce((s, t) => s + t.realized_pnl, 0);
  const totalFees = trades.reduce((s, t) => s + t.fee, 0);

  const labels = equity.length
    ? [timeLabel(equity[0].ts), timeLabel(equity[equity.length - 1].ts)] : [];
  // 2026-08-16 审计修复: 平仓标记只画当前选中 symbol 的交易, 防跨 symbol 错画
  const markers = trades
    .filter(t => t.symbol === symbol)
    .map(t => ({ ts: t.ts, price: t.exit_price, pnl: t.realized_pnl }));

  return (
    <div className="space-y-4">
      {/* 权益曲线 */}
      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-xs uppercase tracking-wider text-gray-400">权益曲线 (USDT)</h3>
          <div className="flex gap-1">
            {[6, 24, 72].map(h => (
              <button key={h} onClick={() => setHours(h)}
                className={`px-2 py-0.5 rounded text-xs ${hours === h ? 'bg-blue-600' : 'bg-gray-700 hover:bg-gray-600'}`}>
                {h}h
              </button>
            ))}
          </div>
        </div>
        <LineChart points={equity.map(p => p.total_equity)} color="#38bdf8"
          formatValue={v => `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`} labels={labels} />
        <div className="grid grid-cols-4 gap-2 mt-3 text-center">
          <div className="bg-gray-900 rounded p-2">
            <div className="text-[10px] text-gray-500">净盈亏</div>
            <div className={`text-sm font-bold ${netPnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {netPnl >= 0 ? '+' : ''}{netPnl.toFixed(2)}
            </div>
          </div>
          <div className="bg-gray-900 rounded p-2">
            <div className="text-[10px] text-gray-500">胜率</div>
            <div className="text-sm font-bold">{trades.length ? `${winRate.toFixed(0)}%` : '—'}</div>
          </div>
          <div className="bg-gray-900 rounded p-2">
            <div className="text-[10px] text-gray-500">盈亏比</div>
            <div className="text-sm font-bold">{profitFactor === Infinity ? '∞' : profitFactor.toFixed(2)}</div>
          </div>
          <div className="bg-gray-900 rounded p-2">
            <div className="text-[10px] text-gray-500">手续费合计</div>
            <div className="text-sm font-bold text-yellow-400">{totalFees.toFixed(2)}</div>
          </div>
        </div>
      </div>

      {/* K线蜡烛图 + 平仓标记 */}
      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex items-center justify-between mb-2 flex-wrap gap-2">
          <h3 className="text-xs uppercase tracking-wider text-gray-400">
            K线图 ({timeframe}, 平仓点已标记)
          </h3>
          <div className="flex gap-1 flex-wrap">
            <div className="flex gap-1 mr-2">
              {['15m', '1h', '4h', '1d', '1w'].map(tf => (
                <button key={tf} onClick={() => setTimeframe(tf)}
                  className={`px-2 py-0.5 rounded text-xs ${timeframe === tf ? 'bg-blue-600' : 'bg-gray-700 hover:bg-gray-600'}`}>
                  {tf}
                </button>
              ))}
            </div>
            <div className="flex gap-1">
              {['BTCUSDT', 'ETHUSDT', 'SOLUSDT'].map(s => (
                <button key={s} onClick={() => setSymbol(s)}
                  className={`px-2 py-0.5 rounded text-xs ${symbol === s ? 'bg-blue-600' : 'bg-gray-700 hover:bg-gray-600'}`}>
                  {s}
                </button>
              ))}
            </div>
          </div>
        </div>
        <KlineChart candles={candles} markers={markers} />
        <p className="text-[10px] text-gray-600 mt-1">
          需 KLINE_ARCHIVE=1 归档 K线 (当前已开启, 数据随 15m 收盘持续积累)
        </p>
      </div>

      {/* 平仓明细 */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">平仓交易明细</h3>
        {!trades.length ? (
          <div className="text-gray-500 text-sm">暂无平仓记录</div>
        ) : (
          <div className="overflow-x-auto max-h-72 overflow-y-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-500 border-b border-gray-700 text-right">
                  <th className="text-left py-1">时间</th>
                  <th className="text-left py-1">交易对</th>
                  <th>方向</th><th>数量</th><th>入场</th><th>出场</th>
                  <th>手续费</th><th>净盈亏</th>
                </tr>
              </thead>
              <tbody>
                {trades.map(t => (
                  <tr key={t.ts} className="border-b border-gray-700/50 text-right">
                    <td className="text-left py-1 text-gray-400 text-xs">{fmtTs(t.ts)}</td>
                    <td className="text-left py-1 font-medium">{t.symbol}</td>
                    <td className={t.direction === 'LONG' ? 'text-green-400' : 'text-red-400'}>
                      {t.direction === 'LONG' ? '多' : '空'}
                    </td>
                    <td>{t.quantity}</td>
                    <td>{t.entry_price?.toLocaleString()}</td>
                    <td>{t.exit_price?.toLocaleString()}</td>
                    <td className="text-yellow-400">{t.fee?.toFixed(3)}</td>
                    <td className={`font-medium ${t.realized_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                      {t.realized_pnl >= 0 ? '+' : ''}{t.realized_pnl.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

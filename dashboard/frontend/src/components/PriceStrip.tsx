/** 实时行情条 (24h ticker, 面板二期 2026-08-16) */
export interface Ticker {
  symbol: string;
  last: number;
  change_pct: number;
  high: number;
  low: number;
}

export function PriceStrip({ tickers, updatedAt }: { tickers: Ticker[]; updatedAt?: number }) {
  if (!tickers || !tickers.length) {
    return (
      <div className="bg-gray-800 rounded-lg p-3 text-xs text-gray-500">
        行情条加载中…（每 10s 刷新）
      </div>
    );
  }
  const ageSec = updatedAt ? Math.max(0, Math.round(Date.now() / 1000 - updatedAt)) : null;
  return (
    <div className="bg-gray-800 rounded-lg p-3">
      <div className="flex flex-wrap gap-x-6 gap-y-1 text-sm">
        {tickers.map(t => (
          <div key={t.symbol} className="flex items-center gap-2">
            <span className="font-medium">{t.symbol}</span>
            <span className="font-mono">{t.last.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
            <span className={`text-xs ${t.change_pct >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {t.change_pct >= 0 ? '+' : ''}{t.change_pct.toFixed(2)}%
            </span>
          </div>
        ))}
      </div>
      <p className="text-[10px] text-gray-600 mt-1">
        {ageSec !== null ? `${ageSec}s 前更新（10s 周期）` : '更新时间未知'}
      </p>
    </div>
  );
}

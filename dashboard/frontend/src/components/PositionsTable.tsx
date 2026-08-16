import { PositionData } from '../hooks/useWebSocket';

const DIR_LABEL: Record<string, string> = { LONG: '多', SHORT: '空' };

export function PositionsTable({ positions }: { positions: PositionData[] }) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">持仓</h2>
      {!positions.length ? (
        <div className="text-gray-500 text-sm">暂无持仓</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-500 border-b border-gray-700">
                <th className="text-left py-1">交易对</th>
                <th className="text-right py-1">方向</th>
                <th className="text-right py-1">数量</th>
                <th className="text-right py-1">杠杆</th>
                <th className="text-right py-1">入场价</th>
                <th className="text-right py-1">保本价</th>
                <th className="text-right py-1">标记价</th>
                <th className="text-right py-1">清算价</th>
                <th className="text-right py-1">爆仓距离</th>
                <th className="text-right py-1">未实现盈亏</th>
              </tr>
            </thead>
            <tbody>
              {positions.map(p => (
                <tr key={p.symbol} className="border-b border-gray-700/50">
                  <td className="py-2 font-medium">{p.symbol}</td>
                  <td className={`text-right ${p.direction === 'LONG' ? 'text-green-400' : 'text-red-400'}`}>
                    {DIR_LABEL[p.direction] ?? p.direction}
                  </td>
                  <td className="text-right">{p.quantity}</td>
                  <td className="text-right">{p.leverage ? `${p.leverage}x` : '—'}</td>
                  <td className="text-right">{p.entry_price?.toLocaleString()}</td>
                  <td className="text-right text-gray-400">
                    {p.break_even ? p.break_even.toLocaleString() : '—'}
                  </td>
                  <td className="text-right">{p.mark_price?.toLocaleString()}</td>
                  <td className={`text-right ${p.liquidation_price ? 'text-orange-400' : 'text-gray-500'}`}>
                    {p.liquidation_price ? p.liquidation_price.toLocaleString() : '—'}
                    {p.adl_quantile ? ` ⚠ADL${p.adl_quantile}` : ''}
                  </td>
                  <td className={`text-right ${p.liq_distance_pct != null && p.liq_distance_pct < 0.08 ? 'text-red-400 font-medium' : 'text-gray-400'}`}>
                    {p.liq_distance_pct != null ? `${(p.liq_distance_pct * 100).toFixed(1)}%` : '—'}
                  </td>
                  <td className={`text-right font-medium ${p.unrealized_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {p.unrealized_pnl >= 0 ? '+' : ''}{p.unrealized_pnl?.toFixed(2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

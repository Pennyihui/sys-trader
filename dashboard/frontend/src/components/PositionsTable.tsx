import { PositionData } from '../hooks/useWebSocket';

export function PositionsTable({ positions }: { positions: PositionData[] }) {
  if (!positions.length) {
    return (
      <div className="bg-gray-800 rounded-lg p-4">
        <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">Positions</h2>
        <div className="text-gray-500 text-sm">No open positions</div>
      </div>
    );
  }
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">Positions</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-500 border-b border-gray-700">
              <th className="text-left py-1">Symbol</th>
              <th className="text-right py-1">Side</th>
              <th className="text-right py-1">Qty</th>
              <th className="text-right py-1">Entry</th>
              <th className="text-right py-1">Mark</th>
              <th className="text-right py-1">Unrealized P&L</th>
            </tr>
          </thead>
          <tbody>
            {positions.map(p => (
              <tr key={p.symbol} className="border-b border-gray-700/50">
                <td className="py-2 font-medium">{p.symbol}</td>
                <td className={`text-right ${p.direction === 'LONG' ? 'text-green-400' : 'text-red-400'}`}>
                  {p.direction}
                </td>
                <td className="text-right">{p.quantity}</td>
                <td className="text-right">{p.entry_price?.toLocaleString()}</td>
                <td className="text-right">{p.mark_price?.toLocaleString()}</td>
                <td className={`text-right font-medium ${p.unrealized_pnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {p.unrealized_pnl >= 0 ? '+' : ''}{p.unrealized_pnl?.toFixed(2)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

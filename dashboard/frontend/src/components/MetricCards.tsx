interface Props { equity: number; positionCount: number; marginRatio: number; dailyPnl: number; }
export function MetricCards({ equity, positionCount, marginRatio, dailyPnl }: Props) {
  const cards = [
    { label: 'Account Equity', value: `$${equity.toLocaleString()}`, color: '' },
    { label: 'Positions', value: `${positionCount}`, color: '' },
    { label: 'Margin Ratio', value: `${(marginRatio * 100).toFixed(1)}%`, color: '' },
    { label: 'Daily P&L', value: `${dailyPnl >= 0 ? '+' : ''}$${dailyPnl.toFixed(2)}`,
      color: dailyPnl >= 0 ? 'text-green-400' : 'text-red-400' },
  ];
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      {cards.map(c => (
        <div key={c.label} className="bg-gray-800 rounded-lg p-4">
          <div className="text-xs text-gray-400 uppercase tracking-wider">{c.label}</div>
          <div className={`text-2xl font-bold mt-1 ${c.color}`}>{c.value}</div>
        </div>
      ))}
    </div>
  );
}

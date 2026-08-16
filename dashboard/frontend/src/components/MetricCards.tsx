interface Props {
  equity: number;
  positionCount: number;
  marginRatio: number;
  dailyPnl: number;
  drawdown: number;
  assets?: { asset: string; walletBalance: number }[];
}
export function MetricCards({ equity, positionCount, marginRatio, dailyPnl, drawdown, assets }: Props) {
  const cards = [
    { label: 'USDT 账户权益', value: `$${equity.toLocaleString(undefined, { maximumFractionDigits: 2 })}`, tone: '' },
    { label: '持仓数', value: `${positionCount}`, tone: '' },
    { label: '保证金率', value: `${(marginRatio * 100).toFixed(1)}%`,
      tone: marginRatio > 0.6 ? 'text-red-400' : marginRatio > 0.4 ? 'text-yellow-400' : 'text-green-400' },
    { label: '当日已实现盈亏', value: `${dailyPnl >= 0 ? '+' : ''}$${dailyPnl.toFixed(2)}`,
      tone: dailyPnl >= 0 ? 'text-green-400' : 'text-red-400' },
    { label: '回撤', value: `${(drawdown * 100).toFixed(2)}%`,
      tone: drawdown > 0.1 ? 'text-red-400' : drawdown > 0.05 ? 'text-yellow-400' : '' },
  ];
  const fmtAsset = (v: number) => v >= 100 ? v.toLocaleString(undefined, { maximumFractionDigits: 0 })
    : v >= 1 ? v.toFixed(4).replace(/0+$/, '').replace(/\.$/, '') : v.toFixed(6);
  return (
    <div>
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        {cards.map(c => (
          <div key={c.label} className="bg-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-400 uppercase tracking-wider">{c.label}</div>
            <div className={`text-2xl font-bold mt-1 ${c.tone}`}>{c.value}</div>
          </div>
        ))}
      </div>
      {assets && assets.length > 0 && (
        <p className="text-[11px] text-gray-500 mt-2">
          资产构成（USDT 权益之外的余额不计入上方权益）:
          {assets.filter(a => a.walletBalance > 0).map(a => (
            <span key={a.asset} className="ml-2">
              {a.asset} <b className="text-gray-300">{fmtAsset(a.walletBalance)}</b>
            </span>
          ))}
        </p>
      )}
    </div>
  );
}

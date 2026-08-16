import type { OrderItem } from '../hooks/useWebSocket';

const SIDE_LABEL: Record<string, string> = { BUY: '买入', SELL: '卖出' };
const STATUS_LABEL: Record<string, string> = {
  FILLED: '已成交',
  PARTIALLY_FILLED: '部分成交',
  NEW: '已挂单',
  PENDING: '待成交',
  CANCELED: '已撤单',
  EXPIRED: '已过期',
  REJECTED: '已拒绝',
  ERROR: '错误',
};

export function OrdersList({ orders, fmtTs }: { orders: OrderItem[]; fmtTs?: (ts?: string) => string }) {
  // 最新在前，最多 5 条
  const recent = orders.slice(-5).reverse();

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">最近订单</h2>
      {!recent.length ? (
        <div className="text-gray-500 text-sm">暂无订单</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-500 border-b border-gray-700">
                <th className="text-left py-1">时间</th>
                <th className="text-left py-1">交易对</th>
                <th className="text-right py-1">方向</th>
                <th className="text-right py-1">数量</th>
                <th className="text-right py-1">价格</th>
                <th className="text-right py-1">状态</th>
              </tr>
            </thead>
            <tbody>
              {recent.map((o, i) => (
                <tr key={`${o.order_id ?? o.symbol}-${i}`} className="border-b border-gray-700/50">
                  <td className="py-2 text-gray-500 text-[11px]">{o.ts ? fmtTs?.(o.ts) : ''}</td>
                  <td className="py-2 font-medium">{o.symbol}</td>
                  <td className={`text-right ${o.side === 'BUY' ? 'text-green-400' : 'text-red-400'}`}>
                    {SIDE_LABEL[o.side ?? ''] ?? o.side}
                  </td>
                  <td className="text-right">{o.quantity}</td>
                  <td className="text-right">{o.price?.toLocaleString()}</td>
                  <td className="text-right">
                    {STATUS_LABEL[o.status ?? ''] ?? o.status}
                    {o.error ? <span className="text-red-400"> ({o.error})</span> : null}
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

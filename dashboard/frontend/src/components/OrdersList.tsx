import type { OrderItem } from '../hooks/useWebSocket';

interface Props {
  orders: OrderItem[];
}

export function OrdersList({ orders }: Props) {
  // 最新在前，最多 5 条
  const recent = orders.slice(-5).reverse();

  if (!recent.length) {
    return (
      <div className="bg-gray-800 rounded-lg p-4">
        <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">Recent Orders</h2>
        <div className="text-gray-500 text-sm">No orders yet</div>
      </div>
    );
  }

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">Recent Orders</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-500 border-b border-gray-700">
              <th className="text-left py-1">Symbol</th>
              <th className="text-right py-1">Side</th>
              <th className="text-right py-1">Qty</th>
              <th className="text-right py-1">Price</th>
              <th className="text-right py-1">Status</th>
            </tr>
          </thead>
          <tbody>
            {recent.map((o, i) => (
              <tr key={`${o.order_id ?? o.symbol}-${i}`} className="border-b border-gray-700/50">
                <td className="py-2 font-medium">{o.symbol}</td>
                <td className={`text-right ${o.side === 'BUY' ? 'text-green-400' : 'text-red-400'}`}>
                  {o.side}
                </td>
                <td className="text-right">{o.quantity}</td>
                <td className="text-right">{o.price?.toLocaleString()}</td>
                <td className="text-right">
                  {o.status}
                  {o.error ? <span className="text-red-400"> ({o.error})</span> : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

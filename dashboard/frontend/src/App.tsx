import { useState } from 'react';
import { useWebSocket } from './hooks/useWebSocket';
import { useAlerts } from './hooks/useAlerts';
import { StatusBar } from './components/StatusBar';
import { MetricCards } from './components/MetricCards';
import { PriceStrip } from './components/PriceStrip';
import { PositionsTable } from './components/PositionsTable';
import { SignalsList } from './components/SignalsList';
import { OrdersList } from './components/OrdersList';
import { ModuleStatus } from './components/ModuleStatus';
import { Controls } from './components/Controls';
import { ParamsPanel } from './components/ParamsPanel';
import { TradingHistory } from './components/TradingHistory';
import { OpsDashboard } from './components/OpsDashboard';

function fmtEventTs(ts?: string): string {
  if (!ts) return '';
  const d = new Date(ts);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`;
}

function TradingDashboard() {
  const { data, connected, lastAck, send } = useWebSocket();
  const alerts = useAlerts(data);

  return (
    <>
      <div className="flex items-center justify-between mb-2">
        <StatusBar connected={connected} />
        {/* 浏览器告警通知开关 */}
        <button onClick={alerts.enable}
          className={`text-xs px-2 py-1 rounded border ${alerts.enabled ? 'bg-green-900 border-green-700 text-green-300' : 'border-gray-600 text-gray-400 hover:text-gray-200'}`}
          title="保证金率/回撤/订单错误时发浏览器通知">
          {alerts.enabled ? '🔔 告警通知已开启' : '🔕 开启告警通知'}
        </button>
      </div>
      {data ? (
        <>
          <MetricCards
            equity={data.equity}
            positionCount={data.position_count}
            marginRatio={data.margin_ratio}
            dailyPnl={data.daily_pnl}
            drawdown={data.drawdown}
            assets={data.assets}
          />
          <p className="text-[11px] text-gray-500 mt-1 mb-2">
            可用余额 <b className="text-gray-300">${data.available_balance.toLocaleString()}</b>
            {data.positions.length > 0 && (
              <> · 已用保证金约 <b className="text-gray-300">
                ${(data.equity * data.margin_ratio).toLocaleString(undefined, { maximumFractionDigits: 2 })}</b></>
            )}
          </p>
          <PriceStrip tickers={data.tickers} updatedAt={data.tickers_updated_at} />
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
            <PositionsTable positions={data.positions} />
            <div className="flex flex-col gap-4">
              <SignalsList signals={data.signals} fmtTs={fmtEventTs} />
              <OrdersList orders={data.orders} fmtTs={fmtEventTs} />
            </div>
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
            <ModuleStatus heartbeats={data.heartbeats} />
            <Controls onCommand={send} lastAck={lastAck} />
          </div>
          <div className="mt-4">
            <ParamsPanel send={send} />
          </div>
          <div className="mt-4">
            <TradingHistory />
          </div>
        </>
      ) : (
        <div className="flex items-center justify-center h-64 text-gray-500">
          等待连接中…
        </div>
      )}
    </>
  );
}

export default function App() {
  const [tab, setTab] = useState<'trading' | 'ops'>('trading');

  return (
    <div className="min-h-screen bg-gray-900 p-4">
      {/* 顶部: 标题 + 标签切换 */}
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-lg font-bold text-gray-200">SysTrader 控制台</h1>
        <div className="flex gap-1 bg-gray-800 rounded-lg p-1">
          <button onClick={() => setTab('trading')}
            className={`px-4 py-1.5 rounded-md text-sm transition-colors ${tab === 'trading' ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-gray-200'}`}>
            交易
          </button>
          <button onClick={() => setTab('ops')}
            className={`px-4 py-1.5 rounded-md text-sm transition-colors ${tab === 'ops' ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-gray-200'}`}>
            运维
          </button>
        </div>
      </div>
      {tab === 'trading' ? <TradingDashboard /> : <OpsDashboard />}
    </div>
  );
}

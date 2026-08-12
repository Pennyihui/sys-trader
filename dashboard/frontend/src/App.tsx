import { useWebSocket } from './hooks/useWebSocket';
import { StatusBar } from './components/StatusBar';
import { MetricCards } from './components/MetricCards';
import { PositionsTable } from './components/PositionsTable';
import { SignalsList } from './components/SignalsList';
import { OrdersList } from './components/OrdersList';
import { ModuleStatus } from './components/ModuleStatus';
import { Controls } from './components/Controls';

export default function App() {
  const { data, connected, lastAck, send } = useWebSocket();

  return (
    <div className="min-h-screen bg-gray-900 p-4">
      <StatusBar connected={connected} />
      {data ? (
        <>
          <MetricCards
            equity={data.equity}
            positionCount={data.position_count}
            marginRatio={data.margin_ratio}
            dailyPnl={data.daily_pnl}
          />
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
            <PositionsTable positions={data.positions} />
            <div className="flex flex-col gap-4">
              <SignalsList signals={data.signals} />
              <OrdersList orders={data.orders} />
            </div>
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
            <ModuleStatus heartbeats={data.heartbeats} />
            <Controls onCommand={send} lastAck={lastAck} />
          </div>
        </>
      ) : (
        <div className="flex items-center justify-center h-64 text-gray-500">
          Waiting for connection...
        </div>
      )}
    </div>
  );
}

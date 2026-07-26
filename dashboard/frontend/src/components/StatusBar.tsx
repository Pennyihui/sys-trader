export function StatusBar({ connected }: { connected: boolean }) {
  return (
    <div className="flex items-center justify-between mb-4 pb-2 border-b border-gray-700">
      <h1 className="text-xl font-bold">SysTrader Dashboard</h1>
      <div className="flex items-center gap-2">
        <span className={`w-3 h-3 rounded-full ${connected ? 'bg-green-400' : 'bg-red-500'}`} />
        <span className="text-sm text-gray-400">{connected ? 'CONNECTED' : 'DISCONNECTED'}</span>
      </div>
    </div>
  );
}

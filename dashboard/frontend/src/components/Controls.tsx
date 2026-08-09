import type { CommandAck } from '../hooks/useWebSocket';

export function Controls({ onCommand, lastAck }: {
  onCommand: (cmd: string) => void;
  lastAck: CommandAck | null;
}) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">Controls</h2>
      <div className="flex gap-2">
        <button onClick={() => onCommand('pause')} className="px-3 py-1.5 rounded bg-yellow-600 hover:bg-yellow-500 text-sm transition-colors">
          Pause
        </button>
        <button onClick={() => onCommand('resume')} className="px-3 py-1.5 rounded bg-green-600 hover:bg-green-500 text-sm transition-colors">
          Resume
        </button>
        {/* Emergency Stop: 熔断停新单 + 撤销活跃订单, 不扁平化持仓 */}
        <button onClick={() => onCommand('emergency_stop')} className="px-3 py-1.5 rounded bg-red-600 hover:bg-red-500 text-sm transition-colors">
          Emergency Stop
        </button>
      </div>
      {lastAck && (
        <p className={`mt-2 text-xs ${lastAck.ok ? 'text-green-400' : 'text-red-400'}`}>
          {lastAck.command}: {lastAck.ok ? 'executed' : `failed — ${lastAck.error || 'unknown error'}`}
        </p>
      )}
    </div>
  );
}

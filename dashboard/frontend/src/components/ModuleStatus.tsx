interface Props {
  heartbeats: Record<string, number>;
}

// 固定 8 模块展示；key 为后端心跳埋点 (MetricsCollector.heartbeat)，
// 仅 market_data / runner / reconciler 三个模块实际在发心跳。
// runner 即主循环（驱动策略分析 → 信号引擎），reconciler 对账循环（驱动组合状态）。
const modules: { key: string | null; label: string }[] = [
  { key: 'market_data', label: 'Market Data' },
  { key: null, label: 'Scheduler' },
  { key: 'runner', label: 'Signal Engine' },
  { key: null, label: 'Risk Manager' },
  { key: null, label: 'Execution' },
  { key: 'reconciler', label: 'Portfolio' },
  { key: null, label: 'Guardian' },
  { key: null, label: 'Monitor' },
];

const ALIVE_AGE = 15; // <15s 视为存活
const STALL_AGE = 60; // >=60s 视为停摆

export function ModuleStatus({ heartbeats }: Props) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">Module Status</h2>
      <div className="grid grid-cols-2 gap-2">
        {modules.map(m => {
          const age = m.key != null ? heartbeats[m.key] : undefined;
          const known = age !== undefined;
          let dotClass = 'bg-gray-600';
          let textClass = 'text-gray-600';
          let status = 'not running';
          if (known) {
            if (age < ALIVE_AGE) {
              dotClass = 'bg-green-400';
              textClass = 'text-green-400';
              status = 'alive';
            } else if (age < STALL_AGE) {
              dotClass = 'bg-yellow-400';
              textClass = 'text-yellow-400';
              status = `${Math.round(age)}s`;
            } else {
              dotClass = 'bg-red-500';
              textClass = 'text-red-400';
              status = 'STALLED';
            }
          }
          return (
            <div key={m.label} className="flex items-center gap-2 text-sm">
              <span className={`w-2 h-2 rounded-full shrink-0 ${dotClass}`} />
              <span>{m.label}</span>
              <span className={`text-xs ml-auto ${textClass}`}>{status}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

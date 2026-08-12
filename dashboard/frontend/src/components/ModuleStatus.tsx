interface Props {
  heartbeats: Record<string, number>;
}

// 固定 8 模块展示；key 为后端心跳埋点 (MetricsCollector.heartbeat)，
// 仅 market_data / runner / reconciler 三个模块实际在发心跳。
// runner 即主循环（驱动策略分析 → 信号引擎），reconciler 对账循环（驱动组合状态）。
//
// Per-module stale 阈值约定 (与 shared/heartbeat_publisher.py docstring 一致):
//   - market_data / runner: 高频心跳 (主循环 5s / 行情消息 ~1s) → <15s 存活, >=60s STALLED
//   - reconciler: 低频对账循环, 心跳周期 300s, age 在 0-300s 间摆动
//     → <300s 存活, >=600s (两个周期没心跳) 才算 STALLED
const modules: { key: string | null; label: string; aliveAge?: number; stallAge?: number }[] = [
  { key: 'market_data', label: 'Market Data', aliveAge: 15, stallAge: 60 },
  { key: null, label: 'Scheduler' },
  { key: 'runner', label: 'Signal Engine', aliveAge: 15, stallAge: 60 },
  { key: null, label: 'Risk Manager' },
  { key: null, label: 'Execution' },
  { key: 'reconciler', label: 'Portfolio', aliveAge: 300, stallAge: 600 },
  { key: null, label: 'Guardian' },
  { key: null, label: 'Monitor' },
];

// 已知心跳 key 的兜底阈值 (未显式配置时使用)
const DEFAULT_ALIVE_AGE = 15;
const DEFAULT_STALL_AGE = 60;

export function ModuleStatus({ heartbeats }: Props) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">Module Status</h2>
      <div className="grid grid-cols-2 gap-2">
        {modules.map(m => {
          const age = m.key != null ? heartbeats[m.key] : undefined;
          const known = age !== undefined;
          const aliveAge = m.aliveAge ?? DEFAULT_ALIVE_AGE;
          const stallAge = m.stallAge ?? DEFAULT_STALL_AGE;
          let dotClass = 'bg-gray-600';
          let textClass = 'text-gray-600';
          let status = 'not running';
          if (known) {
            if (age < aliveAge) {
              dotClass = 'bg-green-400';
              textClass = 'text-green-400';
              status = 'alive';
            } else if (age < stallAge) {
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

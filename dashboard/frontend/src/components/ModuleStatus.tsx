interface Props {
  heartbeats: Record<string, number>;
}

// 系统实际在发心跳的 3 个模块 (MetricsCollector.heartbeat 埋点):
//   market_data (行情消息 ~1s) / runner (主循环 5s) / reconciler (对账 300s)
// 信号引擎/风控链/执行层运行在主运行器进程内, 无独立心跳。
//
// Per-module stale 阈值约定 (与 shared/heartbeat_publisher.py docstring 一致):
//   - market_data / runner: <15s 存活, >=60s STALLED
//   - reconciler: <300s 存活, >=600s (两个周期没心跳) 才算 STALLED
const modules: { key: string; label: string; aliveAge: number; stallAge: number }[] = [
  { key: 'market_data', label: '行情数据', aliveAge: 15, stallAge: 60 },
  { key: 'runner', label: '主运行器', aliveAge: 15, stallAge: 60 },
  { key: 'reconciler', label: '对账器', aliveAge: 300, stallAge: 600 },
];

export function ModuleStatus({ heartbeats }: Props) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">模块状态</h2>
      <div className="space-y-2">
        {modules.map(m => {
          const age = heartbeats[m.key];
          const known = age !== undefined;
          let dotClass = 'bg-gray-600';
          let textClass = 'text-gray-600';
          let status = '未运行';
          if (known) {
            if (age < m.aliveAge) {
              dotClass = 'bg-green-400';
              textClass = 'text-green-400';
              status = '正常';
            } else if (age < m.stallAge) {
              dotClass = 'bg-yellow-400';
              textClass = 'text-yellow-400';
              status = `延迟 ${Math.round(age)}s`;
            } else {
              dotClass = 'bg-red-500';
              textClass = 'text-red-400';
              status = '停滞';
            }
          }
          return (
            <div key={m.key} className="flex items-center gap-2 text-sm">
              <span className={`w-2 h-2 rounded-full shrink-0 ${dotClass}`} />
              <span>{m.label}</span>
              <span className={`text-xs ml-auto ${textClass}`}>{status}</span>
            </div>
          );
        })}
      </div>
      <p className="text-[11px] text-gray-600 mt-3">
        信号引擎/风控链/执行层运行在主运行器进程内，无独立心跳
      </p>
    </div>
  );
}

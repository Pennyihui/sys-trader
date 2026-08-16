import type { SignalItem } from '../hooks/useWebSocket';

// conviction 为 0-1 比例时转百分比；若后端直接给百分比则原样显示
function convictionPct(c?: number): string {
  if (c == null) return '';
  return c <= 1 ? `${Math.round(c * 100)}%` : `${c}%`;
}

export function SignalsList({ signals, fmtTs }: { signals: SignalItem[]; fmtTs?: (ts?: string) => string }) {
  // 最新在前，最多 10 条
  const recent = signals.slice(-10).reverse();

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">最近信号</h2>
      {!recent.length ? (
        <div className="text-gray-500 text-sm">等待信号引擎产生信号…</div>
      ) : (
        <div className="space-y-2 max-h-80 overflow-y-auto">
          {recent.map((s, i) => {
            const isApproved = s.decision === 'signal.approved';
            const isRejected = s.decision === 'signal.rejected';
            const dir = s.direction?.toUpperCase();
            const hasMods = !!s.modifications && Object.keys(s.modifications).length > 0;
            return (
              <div
                key={`${s.signal_id ?? s.symbol}-${i}`}
                className="flex items-center gap-2 text-sm border-b border-gray-700/50 pb-2 last:border-0"
              >
                {fmtTs && s.ts && <span className="text-[10px] text-gray-600 shrink-0">{fmtTs(s.ts)}</span>}
                {isApproved ? (
                  <span className="px-2 py-0.5 rounded text-xs font-medium bg-green-500/20 text-green-400 shrink-0">
                    ✓ 风控通过
                  </span>
                ) : isRejected ? (
                  <span className="px-2 py-0.5 rounded text-xs font-medium bg-red-500/20 text-red-400 shrink-0">
                    ✗ 风控拒绝
                  </span>
                ) : (
                  <span
                    className={`px-2 py-0.5 rounded text-xs font-medium shrink-0 ${
                      dir === 'SHORT' ? 'bg-red-500/20 text-red-400' : 'bg-green-500/20 text-green-400'
                    }`}
                  >
                    {dir === 'SHORT' ? '做空' : '做多'}
                  </span>
                )}
                <span className="font-medium shrink-0">{s.symbol}</span>
                {isApproved ? (
                  <span className="text-xs text-gray-400 truncate">
                    {hasMods ? JSON.stringify(s.modifications) : '无修改'}
                  </span>
                ) : isRejected ? (
                  <span className="text-xs text-gray-400 truncate">{s.reason}</span>
                ) : (
                  <>
                    {s.conviction != null && (
                      <span className="text-xs text-gray-400 shrink-0">确信度 {convictionPct(s.conviction)}</span>
                    )}
                    {s.strategy && <span className="text-xs text-gray-500 truncate">{s.strategy}</span>}
                  </>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

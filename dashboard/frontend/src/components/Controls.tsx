import { useState } from 'react';
import type { CommandAck } from '../hooks/useWebSocket';

/** 危险操作确认对话框 (面板二期 2026-08-16) */
export function Controls({ onCommand, lastAck }: {
  onCommand: (cmd: string) => void;
  lastAck: CommandAck | null;
}) {
  const [symbol, setSymbol] = useState('');
  const [confirm, setConfirm] = useState<{ command: string; sym: string; text: string } | null>(null);

  const askConfirm = (command: string) => {
    const sym = symbol.trim().toUpperCase() || 'ALL';
    const text = command === 'force_exit'
      ? `将市价平仓 ${sym} 的全部持仓并撤销其保护单，确定？`
      : `将撤销 ${sym} 的全部挂单（含 SL/TP 保护单），确定？`;
    setConfirm({ command, sym, text });
  };

  const doConfirm = () => {
    if (!confirm) return;
    onCommand(`${confirm.command}:${confirm.sym}`);
    setConfirm(null);
  };

  const ackLabel = (command: string) =>
    ({ pause: '暂停', resume: '恢复', emergency_stop: '紧急熔断',
       force_exit: '手动平仓', cancel_all: '清场撤单', setparam: '动态参数' } as Record<string, string>)[command] ?? command;

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">控制</h2>
      <div className="flex flex-wrap gap-2">
        <button onClick={() => onCommand('pause')} className="px-3 py-1.5 rounded bg-yellow-600 hover:bg-yellow-500 text-sm transition-colors">
          暂停
        </button>
        <button onClick={() => onCommand('resume')} className="px-3 py-1.5 rounded bg-green-600 hover:bg-green-500 text-sm transition-colors">
          恢复
        </button>
        {/* 紧急熔断: 停新单 + 撤活跃入场单, 保留 SL/TP 保护单 */}
        <button onClick={() => onCommand('emergency_stop')} className="px-3 py-1.5 rounded bg-red-600 hover:bg-red-500 text-sm transition-colors">
          紧急熔断
        </button>
      </div>
      <div className="flex gap-2 mt-3 items-center">
        <input
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          placeholder="交易对 (空=全部)"
          className="w-44 px-2 py-1.5 rounded bg-gray-900 text-sm border border-gray-700 focus:outline-none focus:border-blue-500"
        />
        {/* 危险操作需二次确认 */}
        <button onClick={() => askConfirm('force_exit')} className="px-3 py-1.5 rounded bg-orange-600 hover:bg-orange-500 text-sm transition-colors">
          手动平仓
        </button>
        <button onClick={() => askConfirm('cancel_all')} className="px-3 py-1.5 rounded bg-purple-600 hover:bg-purple-500 text-sm transition-colors">
          清场撤单
        </button>
      </div>
      {lastAck && (
        <p className={`mt-2 text-xs ${lastAck.ok ? 'text-green-400' : 'text-red-400'}`}>
          {ackLabel(lastAck.command)}: {lastAck.ok ? '已执行' : `失败 — ${lastAck.error || '未知错误'}`}
        </p>
      )}

      {/* 确认对话框 */}
      {confirm && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50" onClick={() => setConfirm(null)}>
          <div className="bg-gray-800 border border-red-700 rounded-lg p-5 max-w-sm w-full mx-4" onClick={e => e.stopPropagation()}>
            <h3 className="text-red-400 font-bold mb-2">⚠️ 危险操作确认</h3>
            <p className="text-sm text-gray-300 mb-4">{confirm.text}</p>
            <div className="flex gap-2 justify-end">
              <button onClick={() => setConfirm(null)} className="px-3 py-1.5 rounded bg-gray-700 hover:bg-gray-600 text-sm">
                取消
              </button>
              <button onClick={doConfirm} className="px-3 py-1.5 rounded bg-red-600 hover:bg-red-500 text-sm font-bold">
                确认执行
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

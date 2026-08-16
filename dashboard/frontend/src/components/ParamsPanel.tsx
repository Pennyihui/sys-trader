import { useEffect, useState } from 'react';

/** 参数面板: 展示当前风控参数 + setparam 热更新 (面板二期 2026-08-16) */

interface Params {
  risk_per_trade?: number;
  max_leverage?: number;
}

async function fetchJson<T>(url: string): Promise<T> {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(String(resp.status));
  return resp.json();
}

export function ParamsPanel({ send }: { send: (msg: string) => void }) {
  const [params, setParams] = useState<Params>({});
  const [riskInput, setRiskInput] = useState('');
  const [levInput, setLevInput] = useState('');
  const [feedback, setFeedback] = useState('');

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const s = await fetchJson<{ heartbeat: { risk_per_trade?: number; max_leverage?: number } | null }>(
          '/api/ops/summary');
        if (!alive || !s.heartbeat) return;
        setParams({
          risk_per_trade: s.heartbeat.risk_per_trade,
          max_leverage: s.heartbeat.max_leverage,
        });
      } catch { /* 静默 */ }
    };
    load();
    const t = setInterval(load, 15_000);
    return () => { alive = false; clearInterval(t); };
  }, []);

  const submit = (key: string, value: string) => {
    send(JSON.stringify({ command: 'setparam', key, value }));
    // 2026-08-16: 措辞修正 — 命令经 Redis 异步送达主系统, 生效与否以主系统日志为准
    setFeedback(`已发送 ${key} = ${value} 命令, 等待主系统确认 (重启后回退环境变量值)`);
  };

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">风控参数</h2>
      <div className="flex flex-wrap gap-3 items-center text-sm">
        <span>单笔风险比例:
          <b className="ml-1 text-blue-300">
            {params.risk_per_trade !== undefined ? (params.risk_per_trade * 100).toFixed(2) + '%' : '—'}
          </b>
        </span>
        <input value={riskInput} onChange={e => setRiskInput(e.target.value)}
          placeholder="如 0.01" className="w-20 px-1.5 py-0.5 rounded bg-gray-900 border border-gray-700 text-xs" />
        <button onClick={() => riskInput && submit('risk_per_trade', riskInput)}
          className="px-2 py-0.5 rounded bg-blue-700 hover:bg-blue-600 text-xs">应用</button>

        <span className="ml-2">最大杠杆:
          <b className="ml-1 text-blue-300">{params.max_leverage !== undefined ? params.max_leverage + 'x' : '—'}</b>
        </span>
        <input value={levInput} onChange={e => setLevInput(e.target.value)}
          placeholder="如 5" className="w-14 px-1.5 py-0.5 rounded bg-gray-900 border border-gray-700 text-xs" />
        <button onClick={() => levInput && submit('max_leverage', levInput)}
          className="px-2 py-0.5 rounded bg-blue-700 hover:bg-blue-600 text-xs">应用</button>
      </div>
      {feedback && <p className="text-[11px] text-gray-500 mt-2">{feedback}</p>}
    </div>
  );
}

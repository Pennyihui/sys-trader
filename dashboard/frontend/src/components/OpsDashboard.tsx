import { useEffect, useState } from 'react';
import { LineChart, DualLineChart, timeLabel, fmtTime } from './charts';

interface HbLatest {
  ts: number;
  instance: string;
  kline_closes: number;
  orders_placed: number;
  orders_failed: number;
  server_time_offset: number;
  ws_connected: number;
  ws_total: number;
  funding_cost: number;
  modules: Record<string, number>;
}

interface ProxyStatus {
  status: string;
  total: number;
  healthy: number;
  unhealthy: number;
  [k: string]: unknown;
}

interface NetworkStatus {
  status: string;
  latest: Record<string, unknown>;
  stats_1h: Record<string, unknown>;
  stats_24h: Record<string, unknown>;
}

interface Summary {
  heartbeat: HbLatest | null;
  uptime_seconds: number | null;
  proxy_pool: ProxyStatus;
  network: NetworkStatus;
  log_size_mb: number;
}

interface HistoryPoint {
  ts: number;
  kline_closes: number;
  orders_placed: number;
  orders_failed: number;
  server_time_offset: number;
  ws_connected: number;
  ws_total: number;
  funding_cost: number;
  modules: Record<string, number>;
}

interface SoakRow { ts: number; rss_mb: number; cpu_pct: number; errors_delta: number; }

interface CommandItem { ts: number; source: string; command: string; symbol: string; }
interface AlertItem { ts: number; source: string; message: string; }
interface RestartItem { ts: number; event: string; pid: number; instance: string; }

const HOURS_OPTIONS = [1, 6, 24, 168];

async function fetchJson<T>(url: string): Promise<T> {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`${url}: ${resp.status}`);
  return resp.json();
}

function Kpi({ label, value, tone = '' }: { label: string; value: string; tone?: string }) {
  return (
    <div className="bg-gray-800 rounded-lg p-3">
      <div className="text-[11px] text-gray-400 uppercase tracking-wider">{label}</div>
      <div className={`text-xl font-bold mt-0.5 ${tone}`}>{value}</div>
    </div>
  );
}

function ModuleHealth({ modules }: { modules?: Record<string, number> }) {
  if (!modules) return <p className="text-xs text-gray-500">暂无心跳数据</p>;
  const entries = Object.entries(modules).sort((a, b) => a[0].localeCompare(b[0]));
  const tone = (age: number) => (age <= 15 ? 'text-green-400' : age <= 60 ? 'text-yellow-400' : 'text-red-400');
  return (
    <div className="flex flex-wrap gap-2">
      {entries.map(([name, age]) => (
        <span key={name} className="px-2 py-1 rounded bg-gray-900 text-xs">
          {name} <b className={tone(age)}>{age}s</b>
        </span>
      ))}
    </div>
  );
}

const COMMAND_META: Record<string, { label: string; tone: string }> = {
  emergency_stop: { label: '熔断', tone: 'bg-red-900 text-red-300' },
  resume: { label: '恢复', tone: 'bg-green-900 text-green-300' },
  pause: { label: '暂停', tone: 'bg-yellow-900 text-yellow-300' },
  force_exit: { label: '手动平仓', tone: 'bg-orange-900 text-orange-300' },
  cancel_all: { label: '清场撤单', tone: 'bg-purple-900 text-purple-300' },
  setparam: { label: '动态参数', tone: 'bg-blue-900 text-blue-300' },
};

export function OpsDashboard() {
  const [hours, setHours] = useState(24);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [soak, setSoak] = useState<{ rows: SoakRow[]; total_errors: number }>({ rows: [], total_errors: 0 });
  const [commands, setCommands] = useState<CommandItem[]>([]);
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [restarts, setRestarts] = useState<RestartItem[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const [s, h, k, c, a, r] = await Promise.all([
          fetchJson<Summary>('/api/ops/summary'),
          fetchJson<{ points: HistoryPoint[] }>(`/api/ops/history?hours=${hours}`),
          fetchJson<{ rows: SoakRow[]; total_errors: number }>('/api/ops/soak'),
          fetchJson<{ commands: CommandItem[] }>('/api/ops/commands'),
          fetchJson<{ alerts: AlertItem[] }>('/api/ops/alerts?limit=50'),
          fetchJson<{ restarts: RestartItem[] }>('/api/ops/restarts'),
        ]);
        if (!alive) return;
        setSummary(s); setHistory(h.points); setSoak(k);
        setCommands(c.commands); setAlerts(a.alerts); setRestarts(r.restarts);
        setError('');
      } catch (e) {
        if (alive) setError(String(e));
      }
    };
    load();
    const timer = setInterval(load, 10_000);
    return () => { alive = false; clearInterval(timer); };
  }, [hours]);

  const hb = summary?.heartbeat ?? null;
  const failRate = hb && (hb.orders_placed + hb.orders_failed) > 0
    ? (hb.orders_failed / (hb.orders_placed + hb.orders_failed)) * 100 : 0;
  const labels = history.length
    ? [timeLabel(history[0].ts), timeLabel(history[history.length - 1].ts)] : [];
  const soakLabels = soak.rows.length
    ? [timeLabel(soak.rows[0].ts), timeLabel(soak.rows[soak.rows.length - 1].ts)] : [];
  const lastSoak = soak.rows[soak.rows.length - 1];
  // 时间偏移统计摘要 (面板二期)
  const offsets = history.map(p => p.server_time_offset).filter(v => v !== null);
  const offsetMax = offsets.length ? Math.max(...offsets.map(Math.abs)) : 0;
  const offsetAvg = offsets.length ? offsets.reduce((a, b) => a + b, 0) / offsets.length : 0;

  return (
    <div className="space-y-4">
      {/* 时间范围切换 */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-gray-400">历史窗口:</span>
        {HOURS_OPTIONS.map(h => (
          <button key={h} onClick={() => setHours(h)}
            className={`px-2.5 py-1 rounded text-xs transition-colors ${hours === h ? 'bg-blue-600' : 'bg-gray-800 hover:bg-gray-700'}`}>
            {h >= 168 ? '7d' : `${h}h`}
          </button>
        ))}
        {error && <span className="text-xs text-red-400 ml-2">{error}</span>}
      </div>

      {/* KPI 行 */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
        <Kpi label="最后心跳 (秒前)" value={fmtTime(summary?.uptime_seconds ?? null)}
          tone={(summary?.uptime_seconds ?? 999) > 15 ? 'text-red-400' : 'text-green-400'} />
        <Kpi label="K线闭合" value={hb ? `${hb.kline_closes}` : '—'} />
        <Kpi label="下单 成功/失败" value={hb ? `${hb.orders_placed}/${hb.orders_failed}` : '—'}
          tone={failRate > 10 ? 'text-red-400' : ''} />
        <Kpi label="时间偏移" value={hb ? `${Math.round(hb.server_time_offset)}ms` : '—'}
          tone={Math.abs(hb?.server_time_offset ?? 0) > 3000 ? 'text-yellow-400' : ''} />
        <Kpi label="代理节点 健康/总" value={summary ? `${summary.proxy_pool.healthy}/${summary.proxy_pool.total}` : '—'} />
        <Kpi label="累计错误增量" value={`${soak.total_errors}`}
          tone={soak.total_errors > 0 ? 'text-yellow-400' : ''} />
        <Kpi label="WS 在线/总数" value={hb ? `${hb.ws_connected}/${hb.ws_total}` : '—'}
          tone={hb && hb.ws_connected < hb.ws_total ? 'text-yellow-400' : ''} />
        <Kpi label="资金费成本/8h" value={hb ? `${(hb.funding_cost ?? 0).toFixed(2)} USDT` : '—'}
          tone={(hb?.funding_cost ?? 0) >= 1 ? 'text-yellow-400' : ''} />
        <Kpi label="日志体积" value={summary ? `${summary.log_size_mb} MB` : '—'}
          tone={(summary?.log_size_mb ?? 0) > 500 ? 'text-yellow-400' : ''} />
      </div>

      {/* 图表区 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">K线闭合累计 / 订单失败</h3>
          <DualLineChart
            seriesA={history.map(p => p.kline_closes)}
            seriesB={history.map(p => p.orders_failed)}
            colorA="#34d399" colorB="#f87171" labels={labels} />
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">服务器时间偏移 (ms)</h3>
          <LineChart points={offsets} color="#fbbf24"
            formatValue={v => `${Math.round(v)}ms`} labels={labels} />
          <p className="text-[11px] text-gray-500 mt-1">
            当前窗口: 均值 {offsetAvg.toFixed(0)}ms · 最大 |偏移| {offsetMax.toFixed(0)}ms
          </p>
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">WS 连接数 (在线/总数)</h3>
          <DualLineChart
            seriesA={history.map(p => p.ws_connected || null)}
            seriesB={history.map(p => p.ws_total || null)}
            colorA="#34d399" colorB="#818cf8" labels={labels} />
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">资金费成本 (USDT/8h)</h3>
          <LineChart points={history.map(p => p.funding_cost || null)} color="#a78bfa"
            formatValue={v => v.toFixed(2)} labels={labels} />
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">主进程 RSS (MB, 每小时)</h3>
          <LineChart points={soak.rows.map(r => r.rss_mb)} color="#38bdf8"
            formatValue={v => `${Math.round(v)}MB`} labels={soakLabels} />
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">主进程 CPU (%) + 错误增量</h3>
          <DualLineChart
            seriesA={soak.rows.map(r => r.cpu_pct)}
            seriesB={soak.rows.map(r => r.errors_delta)}
            colorA="#f59e0b" colorB="#f87171" labels={soakLabels} />
          {lastSoak && (
            <p className="text-[11px] text-gray-500 mt-1">
              最近采样 {timeLabel(lastSoak.ts)}: RSS {lastSoak.rss_mb}MB, CPU {lastSoak.cpu_pct}%, 错误 +{lastSoak.errors_delta}
            </p>
          )}
        </div>
      </div>

      {/* 模块健康 + 代理/网络 */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">模块心跳 (秒龄)</h3>
          <ModuleHealth modules={hb?.modules} />
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">代理池</h3>
          {summary ? (
            <div className="text-sm space-y-1">
              <p>状态: <b className={summary.proxy_pool.status === 'running' ? 'text-green-400' : 'text-yellow-400'}>
                {String(summary.proxy_pool.status)}</b></p>
              <p>健康 <b className="text-green-400">{summary.proxy_pool.healthy}</b> /
                 异常 <b className="text-red-400">{summary.proxy_pool.unhealthy}</b> /
                 总计 {summary.proxy_pool.total}</p>
            </div>
          ) : <p className="text-xs text-gray-500">加载中…</p>}
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">网络监控</h3>
          {summary ? (
            <div className="text-sm space-y-1">
              <p>状态: <b className={summary.network.status === 'running' ? 'text-green-400' : 'text-yellow-400'}>
                {String(summary.network.status)}</b></p>
              <p className="text-xs text-gray-500">1h: {JSON.stringify(summary.network.stats_1h)}</p>
              <p className="text-xs text-gray-500">24h: {JSON.stringify(summary.network.stats_24h)}</p>
            </div>
          ) : <p className="text-xs text-gray-500">加载中…</p>}
        </div>
      </div>

      {/* 告警历史 */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">告警历史</h3>
        {alerts.length === 0 ? (
          <p className="text-xs text-gray-500">暂无告警记录（钉钉/看门狗告警会自动归档到这里）</p>
        ) : (
          <ul className="space-y-1 max-h-64 overflow-y-auto">
            {alerts.map((a, i) => (
              <li key={i} className="flex items-start gap-2 text-sm">
                <span className="text-gray-500 text-xs w-14 shrink-0">{timeLabel(a.ts)}</span>
                <span className="px-2 py-0.5 rounded text-xs bg-red-900 text-red-300 shrink-0">
                  {a.source || '告警'}
                </span>
                <span className="text-gray-300 text-xs break-all">{a.message.slice(0, 200)}</span>
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* 重启历史 + 运维命令时间线 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">进程启动/停止历史</h3>
          {restarts.length === 0 ? (
            <p className="text-xs text-gray-500">暂无记录</p>
          ) : (
            <ul className="space-y-1 max-h-64 overflow-y-auto">
              {restarts.map((r, i) => (
                <li key={i} className="flex items-center gap-2 text-sm">
                  <span className="text-gray-500 text-xs w-24">{timeLabel(r.ts)}</span>
                  <span className={`px-2 py-0.5 rounded text-xs ${r.event === 'started' ? 'bg-green-900 text-green-300' : 'bg-gray-700 text-gray-300'}`}>
                    {r.event === 'started' ? '启动' : '停止'}
                  </span>
                  {r.pid > 0 && <span className="text-gray-500 text-xs">PID {r.pid}</span>}
                  {r.instance && <span className="text-gray-500 text-xs">({r.instance})</span>}
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-xs uppercase tracking-wider text-gray-400 mb-2">运维事件时间线</h3>
          {commands.length === 0 ? (
            <p className="text-xs text-gray-500">暂无记录（pause/resume/熔断/平仓等操作会显示在这里）</p>
          ) : (
            <ul className="space-y-1 max-h-64 overflow-y-auto">
              {commands.map((c, i) => {
                const meta = COMMAND_META[c.command] ?? { label: c.command, tone: 'bg-gray-900' };
                return (
                  <li key={i} className="flex items-center gap-2 text-sm">
                    <span className="text-gray-500 text-xs w-14">{timeLabel(c.ts)}</span>
                    <span className={`px-2 py-0.5 rounded text-xs ${meta.tone}`}>{meta.label}</span>
                    {c.symbol && <span className="text-gray-300 text-xs">{c.symbol}</span>}
                    {c.source && <span className="text-gray-600 text-xs">({c.source})</span>}
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}

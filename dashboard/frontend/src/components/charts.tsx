import { useId } from 'react';

/** 轻量 SVG 折线图 (零依赖, 2026-08-16 运维看板) */
export function LineChart({ points, height = 120, color = '#38bdf8', formatValue, labels }: {
  points: (number | null)[];
  height?: number;
  color?: string;
  formatValue?: (v: number) => string;
  labels?: string[];
}) {
  const gid = useId().replace(/:/g, '');
  const w = 600;
  const h = height;
  const pad = 4;

  const valid = points.filter((p): p is number => p !== null);
  if (valid.length === 0) {
    return <div className="text-gray-600 text-xs h-32 flex items-center justify-center">暂无数据</div>;
  }
  let min = Math.min(...valid);
  let max = Math.max(...valid);
  if (min === max) { min -= 1; max += 1; }
  const span = max - min;

  const x = (i: number) => pad + (i / Math.max(1, points.length - 1)) * (w - pad * 2);
  const y = (v: number) => h - pad - ((v - min) / span) * (h - pad * 2);

  // null 分隔成多段 polyline
  const segments: number[][] = [];
  let cur: number[] = [];
  points.forEach((p, i) => {
    if (p === null) { if (cur.length) { segments.push(cur); cur = []; } return; }
    cur.push(i);
  });
  if (cur.length) segments.push(cur);

  const path = segments
    .filter(s => s.length >= 2)
    .map(s => s.map((i, k) => `${k === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${y(points[i] as number).toFixed(1)}`).join(''))
    .join(' ');
  const areaPath = path ? `${path} L${x(points.length - 1).toFixed(1)},${h - pad} L${pad},${h - pad} Z` : '';

  const last = valid[valid.length - 1];
  const fmt = formatValue ?? ((v: number) => v.toFixed(1));

  return (
    <div className="relative">
      <svg viewBox={`0 0 ${w} ${h}`} className="w-full" preserveAspectRatio="none" style={{ height }}>
        <defs>
          <linearGradient id={`g${gid}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.35" />
            <stop offset="100%" stopColor={color} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        {areaPath && <path d={areaPath} fill={`url(#g${gid})`} />}
        {path && <path d={path} fill="none" stroke={color} strokeWidth="1.5" />}
        <text x={w - pad} y={pad + 8} textAnchor="end" fontSize="10" fill={color}>
          {fmt(last)}
        </text>
        <text x={pad} y={pad + 8} textAnchor="start" fontSize="10" fill="#6b7280">
          {fmt(min)}
        </text>
      </svg>
      {labels && labels.length > 0 && (
        <div className="flex justify-between text-[10px] text-gray-500 mt-1">
          <span>{labels[0]}</span>
          <span>{labels[labels.length - 1]}</span>
        </div>
      )}
    </div>
  );
}

/** 双序列对比图 (如 成功 vs 失败) */
export function DualLineChart({ seriesA, seriesB, height = 120, colorA = '#34d399', colorB = '#f87171', labels }: {
  seriesA: (number | null)[];
  seriesB: (number | null)[];
  height?: number;
  colorA?: string;
  colorB?: string;
  labels?: string[];
}) {
  const gid = useId().replace(/:/g, '');
  const w = 600;
  const h = height;
  const pad = 4;
  const all = [...seriesA, ...seriesB].filter((p): p is number => p !== null);
  if (all.length === 0) return <div className="text-gray-600 text-xs h-32 flex items-center justify-center">暂无数据</div>;
  let min = Math.min(...all);
  let max = Math.max(...all);
  if (min === max) { min -= 1; max += 1; }
  const span = max - min;
  const x = (i: number) => pad + (i / Math.max(1, Math.max(seriesA.length, seriesB.length) - 1)) * (w - pad * 2);
  const y = (v: number) => h - pad - ((v - min) / span) * (h - pad * 2);

  const toPath = (series: (number | null)[]) => {
    const segs: string[] = [];
    let cur = '';
    series.forEach((p, i) => {
      if (p === null) { if (cur) { segs.push(cur); cur = ''; } return; }
      cur += `${cur === '' ? 'M' : 'L'}${x(i).toFixed(1)},${y(p).toFixed(1)}`;
    });
    if (cur) segs.push(cur);
    return segs.join(' ');
  };

  return (
    <div>
      <svg viewBox={`0 0 ${w} ${h}`} className="w-full" preserveAspectRatio="none" style={{ height }}>
        <path d={toPath(seriesA)} fill="none" stroke={colorA} strokeWidth="1.5" />
        <path d={toPath(seriesB)} fill="none" stroke={colorB} strokeWidth="1.5" />
      </svg>
      {labels && labels.length > 0 && (
        <div className="flex justify-between text-[10px] text-gray-500 mt-1">
          <span>{labels[0]}</span>
          <span>{labels[labels.length - 1]}</span>
        </div>
      )}
    </div>
  );
}

export function timeLabel(ts: number): string {
  const d = new Date(ts * 1000);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

export function fmtTime(seconds: number | null): string {
  if (seconds === null || seconds === undefined) return '—';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}

// ─── SVG 蜡烛图 (零依赖, 面板二期 2026-08-16; 视觉重做 2026-08-16) ───

export interface Candle {
  open_time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

function fmtCandleTime(ms: number): string {
  const d = new Date(ms);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getMonth() + 1}/${d.getDate()} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

const UP = '#22c55e';
const DOWN = '#ef4444';

export function CandlestickChart({ candles, height = 300, markers }: {
  candles: Candle[];
  height?: number;
  markers?: { ts: number; price: number; pnl: number }[];
}) {
  if (!candles.length) {
    return <div className="text-gray-600 text-xs h-32 flex items-center justify-center">暂无K线数据（需开启 KLINE_ARCHIVE）</div>;
  }
  const w = 760;
  const h = height;
  const padL = 6;
  const padR = 56;   // 右侧价格轴
  const padT = 10;
  const padB = 22;   // 底部时间轴
  const volRatio = 0.22;  // 成交量子图占比
  const plotW = w - padL - padR;
  const volH = (h - padT - padB) * volRatio;
  const priceH = (h - padT - padB) * (1 - volRatio);

  let min = Math.min(...candles.map(c => c.low));
  let max = Math.max(...candles.map(c => c.high));
  const padRange = (max - min) * 0.06 || max * 0.01 || 1;
  min -= padRange;
  max += padRange;
  const span = max - min;
  const maxVol = Math.max(...candles.map(c => c.volume), 1e-9);

  const x = (i: number) => padL + (i + 0.5) * (plotW / candles.length);
  const yPrice = (v: number) => padT + ((max - v) / span) * priceH;
  const slot = plotW / candles.length;
  const bodyW = Math.max(1.2, Math.min(11, slot * 0.62));

  // 水平网格线 + 价格标签 (5 档)
  const gridLevels = [0, 0.25, 0.5, 0.75, 1].map(f => max - f * span);
  // 时间轴标签 (~5 个)
  const xLabelIdx = new Set([0, Math.floor(candles.length * 0.25), Math.floor(candles.length * 0.5),
                             Math.floor(candles.length * 0.75), candles.length - 1]);
  const last = candles[candles.length - 1];

  return (
    <div>
      <svg viewBox={`0 0 ${w} ${h}`} style={{ width: '100%', height: 'auto' }}>
        {/* 网格线 + 价格标签 */}
        {gridLevels.map(v => (
          <g key={v.toFixed(1)}>
            <line x1={padL} x2={w - padR} y1={yPrice(v)} y2={yPrice(v)}
              stroke="#1f2937" strokeWidth="0.6" strokeDasharray="3 3" />
            <text x={w - padR + 6} y={yPrice(v) + 3} fontSize="9.5" fill="#6b7280">
              {v.toLocaleString(undefined, { maximumFractionDigits: 1 })}
            </text>
          </g>
        ))}

        {/* 蜡烛 */}
        {candles.map((c, i) => {
          const up = c.close >= c.open;
          const color = up ? UP : DOWN;
          const bodyTop = yPrice(Math.max(c.open, c.close));
          const bodyH = Math.max(1, Math.abs(yPrice(c.open) - yPrice(c.close)));
          return (
            <g key={c.open_time}>
              <title>{`${fmtCandleTime(c.open_time)} 开:${c.open} 高:${c.high} 低:${c.low} 收:${c.close} 量:${c.volume}`}</title>
              <line x1={x(i)} x2={x(i)} y1={yPrice(c.high)} y2={yPrice(c.low)}
                stroke={color} strokeWidth="0.8" />
              <rect x={x(i) - bodyW / 2} y={bodyTop} width={bodyW} height={bodyH}
                rx="0.8" fill={color} fillOpacity="0.85" />
            </g>
          );
        })}

        {/* 最新价虚线 */}
        <line x1={padL} x2={w - padR} y1={yPrice(last.close)} y2={yPrice(last.close)}
          stroke="#9ca3af" strokeWidth="0.7" strokeDasharray="4 3" />
        <rect x={w - padR} y={yPrice(last.close) - 8} width={padR} height={14} rx="3" fill="#9ca3af" />
        <text x={w - padR + padR / 2} y={yPrice(last.close) + 2} fontSize="9"
          fill="#111827" textAnchor="middle">
          {last.close.toLocaleString(undefined, { maximumFractionDigits: 1 })}
        </text>

        {/* 成交量子图 */}
        <line x1={padL} x2={w - padR} y1={padT + priceH + 2} y2={padT + priceH + 2}
          stroke="#1f2937" strokeWidth="0.6" />
        {candles.map((c, i) => {
          const up = c.close >= c.open;
          const bh = Math.max(1, (c.volume / maxVol) * (volH - 4));
          return (
            <rect key={c.open_time} x={x(i) - bodyW / 2}
              y={h - padB - bh} width={bodyW} height={bh}
              rx="0.5" fill={up ? UP : DOWN} fillOpacity="0.35" />
          );
        })}

        {/* 平仓标记 */}
        {markers?.map((m, i) => {
          const t0 = candles[0].open_time / 1000;
          const t1 = candles[candles.length - 1].open_time / 1000;
          const idx = t1 > t0
            ? Math.min(candles.length - 1, Math.max(0, Math.round((m.ts - t0) / (t1 - t0) * (candles.length - 1))))
            : candles.length - 1;
          const px = x(idx);
          const py = yPrice(m.price);
          return (
            <g key={`m-${i}`}>
              <title>{`平仓 ${m.price} (${m.pnl >= 0 ? '+' : ''}${m.pnl.toFixed(2)})`}</title>
              <circle cx={px} cy={py} r="4.5" fill={m.pnl >= 0 ? UP : DOWN}
                stroke="#111827" strokeWidth="1.2" />
              <path d={`M ${px} ${py - 7} L ${px - 3.5} ${py - 2.5} L ${px + 3.5} ${py - 2.5} Z`}
                fill={m.pnl >= 0 ? UP : DOWN} />
            </g>
          );
        })}

        {/* 时间轴标签 */}
        {[...xLabelIdx].map(i => (
          <text key={i} x={x(i)} y={h - 6} fontSize="9" fill="#6b7280"
            textAnchor={i === 0 ? 'start' : i === candles.length - 1 ? 'end' : 'middle'}>
            {fmtCandleTime(candles[i].open_time)}
          </text>
        ))}
      </svg>
      <div className="flex justify-between text-[10px] text-gray-500 mt-1">
        <span>{fmtCandleTime(candles[0].open_time)}</span>
        <span>绿=涨 红=跌 · 悬浮看 OHLC · ▼/▲=平仓点</span>
        <span>{fmtCandleTime(last.open_time)}</span>
      </div>
    </div>
  );
}

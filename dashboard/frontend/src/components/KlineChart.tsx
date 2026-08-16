import { useEffect, useRef } from 'react';
import {
  createChart, createSeriesMarkers, CandlestickSeries, HistogramSeries,
  ColorType, CrosshairMode, LineStyle, type IChartApi, type UTCTimestamp,
} from 'lightweight-charts';

export interface KlineCandle {
  open_time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface TradeMarker {
  ts: number;
  price: number;
  pnl: number;
}

/** TradingView lightweight-charts 封装 (2026-08-16):
 *  十字光标 / 拖拽平移 / 滚轮缩放 / 成交量子图 / 最新价线 / 平仓标记 */
export function KlineChart({ candles, markers, height = 380 }: {
  candles: KlineCandle[];
  markers?: TradeMarker[];
  height?: number;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const chart = createChart(el, {
      height,
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: '#9ca3af',
        fontSize: 11,
      },
      grid: {
        vertLines: { color: '#1f2937' },
        horzLines: { color: '#1f2937' },
      },
      rightPriceScale: { borderColor: '#374151' },
      timeScale: { borderColor: '#374151', timeVisible: true, secondsVisible: false },
      crosshair: { mode: CrosshairMode.Normal },
    });
    chartRef.current = chart;

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#22c55e', downColor: '#ef4444',
      wickUpColor: '#22c55e', wickDownColor: '#ef4444',
      borderVisible: false,
    });
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    // v5: scaleMargins 是选项而非方法, 经 applyOptions 设置
    chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    const markersApi = createSeriesMarkers(candleSeries, []);

    const setData = () => {
      const data = candles.map(c => ({
        time: Math.floor(c.open_time / 1000) as UTCTimestamp,
        open: c.open, high: c.high, low: c.low, close: c.close,
      }));
      candleSeries.setData(data);
      volumeSeries.setData(candles.map(c => ({
        time: Math.floor(c.open_time / 1000) as UTCTimestamp,
        value: c.volume,
        color: c.close >= c.open ? 'rgba(34,197,94,0.4)' : 'rgba(239,68,68,0.4)',
      })));
      if (markers && markers.length) {
        markersApi.setMarkers(markers.map(m => ({
          time: Math.floor(m.ts) as UTCTimestamp,
          position: m.pnl >= 0 ? 'aboveBar' : 'belowBar',
          color: m.pnl >= 0 ? '#22c55e' : '#ef4444',
          shape: m.pnl >= 0 ? 'arrowUp' : 'arrowDown',
          text: `${m.pnl >= 0 ? '+' : ''}${m.pnl.toFixed(2)}`,
        })));
      } else {
        markersApi.setMarkers([]);
      }
      // 最新价虚线
      const last = candles[candles.length - 1];
      if (last) {
        candleSeries.createPriceLine({
          price: last.close, color: '#9ca3af', lineWidth: 1,
          lineStyle: LineStyle.Dashed, axisLabelVisible: true,
          title: '',
        });
      }
      chart.timeScale().fitContent();
    };
    setData();

    // 容器尺寸自适应
    const ro = new ResizeObserver(() => {
      chart.applyOptions({ width: el.clientWidth });
    });
    ro.observe(el);
    chart.applyOptions({ width: el.clientWidth });

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [candles, markers, height]);

  return (
    <div className="relative">
      <div ref={containerRef} className="w-full" />
      <p className="text-[10px] text-gray-600 mt-1">
        滚轮缩放 · 拖拽平移 · 十字光标 · ▲/▼=平仓点 (盈亏标注) · 底部为成交量
      </p>
    </div>
  );
}

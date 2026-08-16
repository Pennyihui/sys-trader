import { useEffect, useRef, useState } from 'react';
import type { DashboardData, OrderItem } from './useWebSocket';

/** 浏览器告警通知 (面板二期 2026-08-16):
 *  保证金率 > 60% / 回撤 > 10% / 出现 ERROR 订单 → 系统通知 + 提示音。
 *  需用户点击"开启告警"授权 (浏览器要求用户手势)。
 */

function beep() {
  try {
    const Ctx = window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
    const ctx = new Ctx();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'sine';
    osc.frequency.value = 880;
    gain.gain.value = 0.15;
    osc.connect(gain).connect(ctx.destination);
    osc.start();
    osc.stop(ctx.currentTime + 0.25);
    osc.onended = () => ctx.close();
  } catch { /* 静默 */ }
}

export function useAlerts(data: DashboardData | null) {
  const [enabled, setEnabled] = useState(false);
  const [permission, setPermission] = useState<NotificationPermission | 'unsupported'>(
    typeof Notification === 'undefined' ? 'unsupported' : Notification.permission);
  const lastFired = useRef<Record<string, number>>({});

  const enable = async () => {
    if (typeof Notification === 'undefined') return;
    const p = await Notification.requestPermission();
    setPermission(p);
    if (p === 'granted') setEnabled(true);
  };

  useEffect(() => {
    if (!enabled || !data || typeof Notification === 'undefined') return;
    const now = Date.now();
    const fire = (key: string, title: string, body: string) => {
      const last = lastFired.current[key] ?? 0;
      if (now - last < 5 * 60_000) return;  // 5 分钟冷却
      lastFired.current[key] = now;
      try { new Notification(title, { body }); beep(); } catch { /* 静默 */ }
    };
    if (data.margin_ratio > 0.6) {
      fire('margin', '⚠️ 保证金率告警', `保证金率 ${(data.margin_ratio * 100).toFixed(1)}% 超过 60%`);
    }
    if (data.drawdown > 0.1) {
      fire('drawdown', '⚠️ 回撤告警', `回撤 ${(data.drawdown * 100).toFixed(1)}% 超过 10%`);
    }
    const errOrder = (data.orders as OrderItem[]).find(o => o.status === 'ERROR' || o.error);
    if (errOrder) {
      fire(`order-${errOrder.order_id ?? 'x'}`, '❌ 订单错误',
        `${errOrder.symbol} ${errOrder.order_type}: ${errOrder.error || errOrder.status}`);
    }
  }, [data, enabled]);

  return { enabled, permission, enable };
}

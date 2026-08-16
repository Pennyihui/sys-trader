"""HeartbeatPublisher — 周期读取 MetricsCollector 并发布 heartbeat 事件。"""

import logging
import threading
import time

from monitor.collector import MetricsCollector

logger = logging.getLogger(__name__)


class HeartbeatPublisher:
    """周期读取 MetricsCollector 的各模块心跳时间，发布 heartbeat 事件给 dashboard。

    - interval: 发布周期 (秒), 默认 5s
    - instance: 实例标识 (live/paper/...), 随事件携带
    - event_bus: 发布通道; None 时 _run_once 直接返回 (注入模式: 静默)

    Per-module stale 超时约定 (T12 接 Alerter.check_heartbeat 时必须遵守):
      - runner / market_data: <=15s   — 高频心跳 (主循环 5s / 行情消息 ~1s)
      - reconciler: >=600s            — 低频对账循环, 心跳周期 300s
    Alerter.check_heartbeat 默认 60s 超时只适用于高频模块, 低频模块需显式传参。
    """

    def __init__(self, event_bus, interval: float = 5.0, instance: str = "live"):
        self.event_bus = event_bus
        self.interval = interval
        self.instance = instance
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run_once(self):
        if self.event_bus is None:
            return  # 无发布通道 (注入模式), 避免空转
        m = MetricsCollector.instance()
        modules = m.heartbeat_ages()
        # Ops T5: 携带 runner 注册的 gauges 快照, 供 heartbeat_watchdog
        # 检测 K线闭合停滞 / 订单失败率
        stats = {
            "kline_closes": m.get_gauge("kline_closes"),
            "orders_placed": m.get_gauge("orders_placed"),
            "orders_failed": m.get_gauge("orders_failed"),
            "server_time_offset": m.get_gauge("server_time_offset"),
            # 面板二期 (2026-08-16): WS 连接数 / 资金费成本 / 风控参数
            "ws_connected": m.get_gauge("ws_connected"),
            "ws_total": m.get_gauge("ws_total"),
            "funding_cost": m.get_gauge("funding_cost"),
            "risk_per_trade": m.get_gauge("risk_per_trade"),
            "max_leverage": m.get_gauge("max_leverage"),
            # 风控补强 (2026-08-16 #3/#4): 单日交易上限 / 止损距离上限
            "max_trades_day": m.get_gauge("max_trades_day"),
            "max_stop_pct": m.get_gauge("max_stop_pct"),
        }
        self.event_bus.publish("heartbeat", {
            "instance": self.instance, "modules": modules, "stats": stats,
        })

    def start(self):
        if self._thread and self._thread.is_alive():
            logger.warning("HeartbeatPublisher already running")
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        while not self._stop.is_set():
            try:
                self._run_once()
            except Exception as e:
                logger.warning("HeartbeatPublisher error: %s", e, exc_info=True)
            self._stop.wait(timeout=self.interval)

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

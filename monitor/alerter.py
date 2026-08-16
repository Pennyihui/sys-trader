"""Alerter — threshold checking and alert dispatching."""

import threading
import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional
from monitor.collector import MetricsCollector

logger = logging.getLogger(__name__)


class AlertLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class Alert:
    level: AlertLevel
    metric: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class Alerter:
    # 同 metric 告警节流窗口 (秒): 心跳/阈值循环高频调用, 无节流会告警风暴
    THROTTLE_SECONDS = 60.0
    MAX_ALERTS = 500  # 列表上限, 防止长跑无限增长

    def __init__(self, on_alert: Optional[Callable[[Alert], None]] = None):
        self.on_alert = on_alert or (lambda a: None)
        self._alerts: list[Alert] = []
        self._last_fired: Dict[str, float] = {}
        # 2026-08-16 审计: fire() 无锁, 多线程告警并发写 _alerts/_last_fired
        self._lock = threading.Lock()

    def fire(self, level: AlertLevel, metric: str, message: str, context: Optional[dict] = None):
        # 节流: 同 metric 在窗口内重复触发只记 DEBUG, 不重复推送
        now = time.time()
        with self._lock:
            last = self._last_fired.get(metric, 0.0)
            if now - last < self.THROTTLE_SECONDS:
                logger.debug("ALERT throttled [%s] %s", level.value, metric)
                return None
            self._last_fired[metric] = now
        alert = Alert(level=level, metric=metric, message=message, context=context or {})
        with self._lock:
            self._alerts.append(alert)
            if len(self._alerts) > self.MAX_ALERTS:
                self._alerts = self._alerts[-self.MAX_ALERTS:]
        self.on_alert(alert)
        logger.log(
            {"INFO": 20, "WARNING": 30, "CRITICAL": 40}.get(level.value, 20),
            "ALERT [%s] %s: %s", level.value, metric, message,
        )
        return alert

    def check_heartbeat(self, module: str, collector: MetricsCollector, timeout_seconds: int = 60):
        """检查模块心跳是否超时, 超时发 CRITICAL 告警。

        Per-module timeout 约定 (与 shared/heartbeat_publisher.py 保持一致):
          - runner / market_data: <=15s   — 高频心跳 (主循环 5s / 行情消息 ~1s)
          - reconciler: >=600s            — 低频对账循环, 心跳周期 300s
        默认 60s 只适用于高频模块; 检查 reconciler 时必须显式传 >=600s,
        否则低频心跳会被误判为超时。
        """
        last = collector.last_heartbeat(module)
        if last is None:
            self.fire(AlertLevel.WARNING, f"heartbeat.{module}", f"No heartbeat ever received from {module}")
        elif time.time() - last > timeout_seconds:
            self.fire(AlertLevel.CRITICAL, f"heartbeat.{module}", f"{module} heartbeat timeout: {time.time() - last:.0f}s since last beat")

    def check_thresholds(self, collector: MetricsCollector, portfolio: Any = None):
        if portfolio is None:
            return
        # 属性缺失防御: 传入对象没有 margin_ratio/current_drawdown 时
        # 直接跳过, 不再 AttributeError 炸掉监控循环 (2026-08-16 审计)。
        margin_ratio = getattr(portfolio, "margin_ratio", None)
        if margin_ratio is not None:
            if margin_ratio > 0.80:
                self.fire(AlertLevel.CRITICAL, "margin_ratio", f"Margin ratio {margin_ratio:.1%} > 80%", {"margin_ratio": margin_ratio})
            elif margin_ratio > 0.60:
                self.fire(AlertLevel.WARNING, "margin_ratio", f"Margin ratio {margin_ratio:.1%} > 60%", {"margin_ratio": margin_ratio})

        dd = getattr(portfolio, "current_drawdown", None)
        if dd is not None:
            if dd > 0.15:
                self.fire(AlertLevel.CRITICAL, "drawdown", f"Drawdown {dd:.1%} > 15%", {"drawdown": dd})
            elif dd > 0.10:
                self.fire(AlertLevel.WARNING, "drawdown", f"Drawdown {dd:.1%} > 10%", {"drawdown": dd})

    def recent_alerts(self, n: int = 10) -> list[Alert]:
        return self._alerts[-n:]

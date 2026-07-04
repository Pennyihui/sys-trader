"""Alerter — threshold checking and alert dispatching."""

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
    def __init__(self, on_alert: Optional[Callable[[Alert], None]] = None):
        self.on_alert = on_alert or (lambda a: None)
        self._alerts: list[Alert] = []

    def fire(self, level: AlertLevel, metric: str, message: str, context: Optional[dict] = None):
        alert = Alert(level=level, metric=metric, message=message, context=context or {})
        self._alerts.append(alert)
        self.on_alert(alert)
        logger.log(
            {"INFO": 20, "WARNING": 30, "CRITICAL": 40}.get(level.value, 20),
            "ALERT [%s] %s: %s", level.value, metric, message,
        )
        return alert

    def check_heartbeat(self, module: str, collector: MetricsCollector, timeout_seconds: int = 60):
        last = collector.last_heartbeat(module)
        if last is None:
            self.fire(AlertLevel.WARNING, f"heartbeat.{module}", f"No heartbeat ever received from {module}")
        elif time.time() - last > timeout_seconds:
            self.fire(AlertLevel.CRITICAL, f"heartbeat.{module}", f"{module} heartbeat timeout: {time.time() - last:.0f}s since last beat")

    def check_thresholds(self, collector: MetricsCollector, portfolio: Any = None):
        if portfolio is not None:
            margin_ratio = portfolio.margin_ratio
            if margin_ratio > 0.80:
                self.fire(AlertLevel.CRITICAL, "margin_ratio", f"Margin ratio {margin_ratio:.1%} > 80%", {"margin_ratio": margin_ratio})
            elif margin_ratio > 0.60:
                self.fire(AlertLevel.WARNING, "margin_ratio", f"Margin ratio {margin_ratio:.1%} > 60%", {"margin_ratio": margin_ratio})

            dd = portfolio.current_drawdown
            if dd > 0.15:
                self.fire(AlertLevel.CRITICAL, "drawdown", f"Drawdown {dd:.1%} > 15%", {"drawdown": dd})
            elif dd > 0.10:
                self.fire(AlertLevel.WARNING, "drawdown", f"Drawdown {dd:.1%} > 10%", {"drawdown": dd})

    def recent_alerts(self, n: int = 10) -> list[Alert]:
        return self._alerts[-n:]

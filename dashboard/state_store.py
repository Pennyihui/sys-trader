"""StateStore — EventBus 消费侧：维护 dashboard 所需的系统状态副本。"""

import logging
import threading
import time
from typing import Dict, List, Optional

from shared.event_bus import EventBus

logger = logging.getLogger(__name__)

STREAMS = [
    "position.changed", "order.filled", "signal.generated",
    "signal.approved", "signal.rejected", "heartbeat",
]


class StateStore:
    def __init__(self, event_bus: EventBus, instance_filter: str = "live",
                 max_signals: int = 50):
        self.bus = event_bus
        self.instance_filter = instance_filter
        self.max_signals = max_signals
        self._lock = threading.Lock()
        self.positions: Dict[str, dict] = {}
        self.equity: float = 0.0
        self.margin_ratio: float = 1.0
        self.daily_pnl: float = 0.0
        self.drawdown: float = 0.0
        self.signals: List[dict] = []
        self.orders: List[dict] = []
        self.heartbeats: Dict[str, float] = {}
        self._threads: List[threading.Thread] = []

    def start(self):
        for stream in STREAMS:
            t = threading.Thread(
                target=self.bus.run_consumer,
                args=(stream, "dashboard", self._handle, 5, 100),
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        logger.info("StateStore consuming %d streams", len(STREAMS))

    def stop(self):
        """仅 join 本实例的消费线程，不触碰共享 bus。

        注意：bus.stop() 会停掉 EventBus 上所有消费者——Task 12 注入
        共享 EventBus 后调用即杀全系统消费者，故这里不调用。消费线程是
        daemon，join 超时后随进程退出；per-consumer 优雅停止留待后续。
        """
        deadline = time.time() + 3
        for t in self._threads:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            t.join(timeout=remaining)
        self._threads.clear()

    def _should_accept(self, data: dict) -> bool:
        inst = data.get("instance", "live")
        return inst == self.instance_filter

    def _handle(self, event):
        if isinstance(event, dict):
            stream = event.get("stream", "")
            data = event.get("data", {})
        else:
            stream = getattr(event, "stream", "")
            data = getattr(event, "data", {}) or {}
        if not self._should_accept(data):
            return
        with self._lock:
            if stream == "position.changed":
                self._on_position(data)
            elif stream == "signal.generated":
                self.signals.append(data)
                self.signals = self.signals[-self.max_signals:]
            elif stream == "order.filled":
                self.orders.append(data)
                self.orders = self.orders[-self.max_signals:]
            elif stream == "heartbeat":
                self.heartbeats.update(data.get("modules", {}))
            elif stream in ("signal.approved", "signal.rejected"):
                self.signals.append({"decision": stream, **data})
                self.signals = self.signals[-self.max_signals:]

    def _update_metrics(self, data: dict):
        for attr, key in (("margin_ratio", "margin_ratio"),
                          ("daily_pnl", "daily_pnl"),
                          ("drawdown", "drawdown")):
            if data.get(key) is not None:
                setattr(self, attr, data[key])

    def _on_position(self, data: dict):
        event = data.get("event")
        if event == "open":
            self.positions[data["symbol"]] = data
        elif event == "close":
            self.positions.pop(data["symbol"], None)
            if data.get("total_equity") is not None:
                self.equity = data["total_equity"]
            self._update_metrics(data)
        elif event == "equity":
            if data.get("total_equity") is not None:
                self.equity = data["total_equity"]
            self._update_metrics(data)

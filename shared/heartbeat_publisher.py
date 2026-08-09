"""HeartbeatPublisher — 周期读取 MetricsCollector 并发布 heartbeat 事件。"""

import logging
import threading
import time

logger = logging.getLogger(__name__)


class HeartbeatPublisher:
    """周期读取 MetricsCollector 的各模块心跳时间，发布 heartbeat 事件给 dashboard。

    - interval: 发布周期 (秒), 默认 5s
    - instance: 实例标识 (live/paper/...), 随事件携带
    - event_bus: 发布通道; None 时 _run_once 跳过 publish (静默)
    """

    def __init__(self, event_bus, interval: float = 5.0, instance: str = "live"):
        self.event_bus = event_bus
        self.interval = interval
        self.instance = instance
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run_once(self):
        from monitor.collector import MetricsCollector
        collector = MetricsCollector.instance()
        modules = {}
        with collector._lock:
            for mod, ts in collector._heartbeats.items():
                modules[mod] = round(time.time() - ts, 1)
        if self.event_bus is not None:
            self.event_bus.publish("heartbeat", {
                "instance": self.instance, "modules": modules,
            })

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        while not self._stop.is_set():
            try:
                self._run_once()
            except Exception as e:
                logger.warning("HeartbeatPublisher error: %s", e)
            self._stop.wait(timeout=self.interval)

    def stop(self):
        self._stop.set()

"""MetricsCollector -- thread-safe singleton for heartbeat, counters, and gauges."""

import time
import threading
from typing import Optional


class MetricsCollector:
    _instance: Optional["MetricsCollector"] = None
    _class_lock: threading.Lock = threading.Lock()

    def __init__(self):
        self._heartbeats: dict[str, float] = {}
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._lock = threading.Lock()

    @classmethod
    def instance(cls) -> "MetricsCollector":
        if cls._instance is None:
            with cls._class_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls):
        with cls._class_lock:
            cls._instance = None

    def heartbeat(self, module: str):
        with self._lock:
            self._heartbeats[module] = time.time()

    def last_heartbeat(self, module: str) -> Optional[float]:
        with self._lock:
            return self._heartbeats.get(module)

    def heartbeat_ages(self) -> dict[str, float]:
        """返回 {module: age_seconds} 快照副本（锁内读取, 取整到 0.1s）。

        供 HeartbeatPublisher 等外部模块读取心跳年龄,
        避免直接访问私有成员 _heartbeats/_lock。
        """
        now = time.time()
        with self._lock:
            return {mod: round(now - ts, 1) for mod, ts in self._heartbeats.items()}

    def increment(self, metric: str, amount: int = 1):
        with self._lock:
            self._counters[metric] = self._counters.get(metric, 0) + amount

    def get_counter(self, metric: str) -> int:
        with self._lock:
            return self._counters.get(metric, 0)

    def set_gauge(self, metric: str, value: float):
        with self._lock:
            self._gauges[metric] = value

    def get_gauge(self, metric: str) -> float:
        with self._lock:
            return self._gauges.get(metric, 0.0)

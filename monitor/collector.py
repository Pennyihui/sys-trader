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

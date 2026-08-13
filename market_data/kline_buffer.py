"""K-line buffer -- stores recent candles, detects closure."""

from dataclasses import dataclass
from threading import Lock
from typing import List, Optional
from collections import defaultdict


@dataclass
class Kline:
    symbol: str
    timeframe: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool = False


class KlineBuffer:
    def __init__(self, max_size: int = 500):
        self.max_size = max_size
        self._data: dict[str, List[Kline]] = defaultdict(list)
        self._latest: dict[str, Kline] = {}
        self._lock = Lock()

    def _key(self, symbol: str, timeframe: str) -> str:
        return f"{symbol}:{timeframe}"

    def add(self, kline: Kline):
        key = self._key(kline.symbol, kline.timeframe)
        with self._lock:
            existing = self._latest.get(key)
            if existing and existing.open_time == kline.open_time:
                self._data[key][-1] = kline
            else:
                self._data[key].append(kline)
                if len(self._data[key]) > self.max_size:
                    self._data[key] = self._data[key][-self.max_size:]
            self._latest[key] = kline

    def get_klines(self, symbol: str, timeframe: str, limit: int = 100) -> List[Kline]:
        with self._lock:
            kl = list(self._data.get(self._key(symbol, timeframe), []))
        return kl[-limit:] if limit < len(kl) else kl

    def count(self, symbol: str, timeframe: str) -> int:
        with self._lock:
            return len(self._data.get(self._key(symbol, timeframe), []))

    def is_closed(self, symbol: str, timeframe: str, open_time: int) -> bool:
        with self._lock:
            latest = self._latest.get(self._key(symbol, timeframe))
            if latest and latest.open_time == open_time:
                return latest.is_closed
            return False

    def get_latest(self, symbol: str, timeframe: str) -> Optional[Kline]:
        with self._lock:
            return self._latest.get(self._key(symbol, timeframe))

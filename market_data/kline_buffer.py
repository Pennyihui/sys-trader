"""K-line buffer -- stores recent candles, detects closure."""

from dataclasses import dataclass
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

    def _key(self, symbol: str, timeframe: str) -> str:
        return f"{symbol}:{timeframe}"

    def add(self, kline: Kline):
        key = self._key(kline.symbol, kline.timeframe)
        existing = self._latest.get(key)
        if existing and existing.open_time == kline.open_time:
            self._data[key][-1] = kline
        else:
            self._data[key].append(kline)
            if len(self._data[key]) > self.max_size:
                self._data[key] = self._data[key][-self.max_size:]
        self._latest[key] = kline

    def get_klines(self, symbol: str, timeframe: str, limit: int = 100) -> List[Kline]:
        key = self._key(symbol, timeframe)
        kl = self._data.get(key, [])
        return kl[-limit:] if limit < len(kl) else list(kl)

    def count(self, symbol: str, timeframe: str) -> int:
        return len(self._data.get(self._key(symbol, timeframe), []))

    def is_closed(self, symbol: str, timeframe: str, open_time: int) -> bool:
        key = self._key(symbol, timeframe)
        latest = self._latest.get(key)
        if latest and latest.open_time == open_time:
            return latest.is_closed
        return False

    def get_latest(self, symbol: str, timeframe: str) -> Optional[Kline]:
        return self._latest.get(self._key(symbol, timeframe))

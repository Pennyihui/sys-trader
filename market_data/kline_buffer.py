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

    def add(self, kline: Kline) -> bool:
        """写入 K 线。返回 True=已写入，False=乱序丢弃。

        乱序保护（2026-08-16 审计修复）：备用连接重连窗口可能补发
        open_time 早于当前最新 K 线的过期闭合 candle，直接 append 会破坏
        序列（`_latest` 指向旧 candle → 指标/信号基于乱序数据）。
        过期 K 线一律丢弃，宁可少一根也不让序列乱序。
        """
        key = self._key(kline.symbol, kline.timeframe)
        with self._lock:
            existing = self._latest.get(key)
            if existing and existing.open_time == kline.open_time:
                self._data[key][-1] = kline
            elif existing and kline.open_time < existing.open_time:
                # 乱序/过期 K 线: 先按 open_time 找同窗行（倒序扫描，列表 ≤ max_size）
                rows = self._data[key]
                for i in range(len(rows) - 1, -1, -1):
                    if rows[i].open_time == kline.open_time:
                        rows[i] = kline
                        return True
                # 无同窗行 → 纯过期数据，丢弃
                return False
            else:
                self._data[key].append(kline)
                if len(self._data[key]) > self.max_size:
                    self._data[key] = self._data[key][-self.max_size:]
            self._latest[key] = kline
        return True

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

    def all_entries(self) -> dict:
        """全部 (key, K线列表) 快照 — 主备切换补发漏通知用 (2026-08-16)。"""
        with self._lock:
            return {k: list(v) for k, v in self._data.items()}

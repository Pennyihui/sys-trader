"""ReplayFeed — 从本地 JSON K 线文件重放，实现 MarketDataFeed 的行情接口。

供离线模拟使用：驱动完整装配（DRY_RUN）跑历史数据，验证链路逻辑与内存稳定性。
JSON 文件格式: {SYMBOL}_{TIMEFRAME}.json — JSON 数组，每元素含
open/high/low/close/volume/open_time（可选 close_time）。

重放语义与实盘对齐（quality review 2026-08-10）：
  - 按 open_time 全局排序，逐根 K 线闭合触发 on_kline_closed（跨 symbol 交错），
    每根一次回调 —— 与实盘 kline.closed 语义一致，逐 bar 行为（持仓去重 /
    PENDING 去重 / DrawdownBreaker 冷却 / DailyLossLimit 累计）得到覆盖。
  - 回调携带最近 BUFFER_WINDOW 根滑动窗口（每 symbol 独立 deque）——
    与实盘 KlineBuffer.get_klines(limit=100) 的回调窗口一致。
"""

import json
import logging
import os
import threading
from collections import deque
from typing import Callable, Dict, List, Optional

from market_data.kline_buffer import Kline

logger = logging.getLogger(__name__)

BUFFER_WINDOW = 100  # 回调滑动窗口根数，与实盘 KlineBuffer 回调窗口一致 (feed.py limit=100)


class ReplayFeed:
    def __init__(self, data_dir: str, symbols: List[str], timeframe: str = "15m",
                 on_kline_closed: Optional[Callable] = None):
        self.data_dir = data_dir
        self.symbols = symbols
        self.timeframe = timeframe
        self.on_kline_closed = on_kline_closed or (lambda s, tf, ohlcv: None)
        self._prices: Dict[str, float] = {}
        self._klines: Dict[str, List[dict]] = {}
        self._buffers: Dict[str, deque] = {}
        self._conns: List = []  # 兼容 run_forever 的 _check_connections/_snapshot
        self._stop = threading.Event()

    def _load(self):
        for sym in self.symbols:
            path = os.path.join(self.data_dir, f"{sym}_{self.timeframe}.json")
            with open(path) as f:
                self._klines[sym] = json.load(f)
        self._buffers = {sym: deque(maxlen=BUFFER_WINDOW) for sym in self.symbols}

    def start(self):
        self._load()

    def stop(self):
        self._stop.set()

    def backfill(self, limit: int = 100, timeframes: Optional[List[str]] = None):
        """数据已在 start() 加载完毕，无 REST 回填。

        兼容 SystemRunner.initialize() 的统一调用（真实 feed 用 REST 拉历史，
        重放数据直接来自本地文件，无需回填）。
        """

    @staticmethod
    def _to_kline(sym: str, timeframe: str, row: dict) -> Kline:
        return Kline(
            symbol=sym, timeframe=timeframe,
            open_time=int(row["open_time"]),
            close_time=int(row.get("close_time", 0)),
            open=float(row["open"]), high=float(row["high"]),
            low=float(row["low"]), close=float(row["close"]),
            volume=float(row["volume"]), is_closed=True,
        )

    def run_once(self, on_bar: Optional[Callable] = None):
        """按 open_time 全局排序逐根重放：每根 K 线闭合触发一次 on_kline_closed。

        - 跨 symbol 交错：全部 (symbol, kline) 按 (open_time, symbol) 排序，保证确定性。
        - 回调携带最近 BUFFER_WINDOW 根（滑动窗口），与实盘 KlineBuffer 一致。
        - on_bar: 每根处理后的回调 (累计根数, symbol, kline)，供 RSS 采样/进度统计。
        - 重复调用幂等：每次 run_once 从空窗口重新重放。
        """
        self._buffers = {sym: deque(maxlen=BUFFER_WINDOW) for sym in self.symbols}
        bars = sorted(
            ((int(r["open_time"]), sym, r)
             for sym, rows in self._klines.items() for r in rows),
            key=lambda b: (b[0], b[1]),
        )
        for idx, (_, sym, row) in enumerate(bars, start=1):
            kline = self._to_kline(sym, self.timeframe, row)
            self._buffers[sym].append(kline)
            self._prices[sym] = kline.close
            self.on_kline_closed(sym, self.timeframe, list(self._buffers[sym]))
            if on_bar is not None:
                on_bar(idx, sym, kline)
        return self._klines

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

    def get_mark_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

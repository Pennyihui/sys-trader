"""ReplayFeed — 从本地 JSON K 线文件重放，实现 MarketDataFeed 的行情接口。

供离线模拟使用：驱动完整装配（DRY_RUN）跑历史数据，验证链路逻辑与内存稳定性。
JSON 文件格式: {SYMBOL}_{TIMEFRAME}.json — JSON 数组，每元素含
open/high/low/close/volume/open_time（可选 close_time）。
"""

import json
import logging
import os
import threading
from typing import Callable, Dict, List, Optional

from market_data.kline_buffer import Kline

logger = logging.getLogger(__name__)


class ReplayFeed:
    def __init__(self, data_dir: str, symbols: List[str], timeframe: str = "15m",
                 on_kline_closed: Optional[Callable] = None):
        self.data_dir = data_dir
        self.symbols = symbols
        self.timeframe = timeframe
        self.on_kline_closed = on_kline_closed or (lambda s, tf, ohlcv: None)
        self._prices: Dict[str, float] = {}
        self._klines: Dict[str, List[dict]] = {}
        self._stop = threading.Event()

    def _load(self):
        for sym in self.symbols:
            path = os.path.join(self.data_dir, f"{sym}_{self.timeframe}.json")
            with open(path) as f:
                self._klines[sym] = json.load(f)

    def start(self):
        self._load()

    def stop(self):
        self._stop.set()

    def backfill(self, limit: int = 100, timeframes: Optional[List[str]] = None):
        """数据已在 start() 加载完毕，无 REST 回填。

        兼容 SystemRunner.initialize() 的统一调用（真实 feed 用 REST 拉历史，
        重放数据直接来自本地文件，无需回填）。
        """

    def run_once(self):
        """按时间顺序重放全部 K 线：每个 symbol 触发一次 on_kline_closed（全量历史）。

        ohlcv 转为真实 Kline 对象（k.open/k.high/... 属性访问）——
        runner._on_kline_closed 依赖该接口构造 DataFrame，传 dict 会静默失败。
        """
        for sym, rows in self._klines.items():
            if not rows:
                continue
            klines = [
                Kline(
                    symbol=sym, timeframe=self.timeframe,
                    open_time=int(r["open_time"]),
                    close_time=int(r.get("close_time", 0)),
                    open=float(r["open"]), high=float(r["high"]),
                    low=float(r["low"]), close=float(r["close"]),
                    volume=float(r["volume"]), is_closed=True,
                )
                for r in rows
            ]
            self._prices[sym] = klines[-1].close
            self.on_kline_closed(sym, self.timeframe, klines)
        return self._klines

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

    def get_mark_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

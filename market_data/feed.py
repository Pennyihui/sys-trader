"""MarketDataFeed -- WebSocket -> Kline buffer -> kline.closed events."""

import json
import time
import threading
import logging
from typing import Dict, List, Optional
from market_data.kline_buffer import KlineBuffer, Kline

logger = logging.getLogger(__name__)


class MarketDataFeed:
    def __init__(self, symbols: List[str], testnet: bool = True, on_kline_closed=None):
        self.symbols = symbols
        self.testnet = testnet
        self.buffer = KlineBuffer(max_size=500)
        self.on_kline_closed = on_kline_closed or (lambda symbol, timeframe, ohlcv: None)
        self._mark_prices: Dict[str, float] = {}
        self._running = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _timeframe_from_interval(self, interval: str) -> str:
        mapping = {"1w": "1w", "1d": "1d", "4h": "4h"}
        return mapping.get(interval, interval)

    def _stream_timeframe_map(self, symbols: List[str]) -> Dict[str, str]:
        m = {}
        for sym in symbols:
            s = sym.lower()
            m[f"{s}@kline_4h"] = "4h"
            m[f"{s}@kline_1d"] = "1d"
            m[f"{s}@kline_1w"] = "1w"
        return m

    def _on_kline_message(self, msg: dict):
        k = msg.get("k", {})
        symbol = msg.get("s", "").upper()
        interval = k.get("i", "4h")
        timeframe = self._timeframe_from_interval(interval)
        kline = Kline(
            symbol=symbol, timeframe=timeframe,
            open_time=k.get("t", 0), close_time=k.get("T", 0),
            open=float(k.get("o", 0)), high=float(k.get("h", 0)),
            low=float(k.get("l", 0)), close=float(k.get("c", 0)),
            volume=float(k.get("v", 0)),
            is_closed=k.get("x", False),
        )
        prev_closed = self.buffer.is_closed(symbol, timeframe, kline.open_time)
        self.buffer.add(kline)
        if kline.is_closed and not prev_closed:
            ohlcv = self.buffer.get_klines(symbol, timeframe, limit=100)
            self.on_kline_closed(symbol, timeframe, ohlcv)

    def _on_mark_price_message(self, msg: dict):
        symbol = msg.get("s", "").upper()
        price = float(msg.get("p", 0))
        self._mark_prices[symbol] = price

    def get_mark_price(self, symbol: str) -> Optional[float]:
        return self._mark_prices.get(symbol.upper())

    def start(self):
        self._running = True
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while self._running and not self._stop.is_set():
            logger.info("MarketDataFeed running (WebSocket integration pending)")
            self._stop.wait(timeout=60)

    def stop(self):
        self._running = False
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

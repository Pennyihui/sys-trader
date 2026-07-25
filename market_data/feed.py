"""MarketDataFeed — Binance Futures WebSocket -> Kline buffer -> kline.closed events."""

import json
import time
import threading
import logging
from typing import Callable, Dict, List, Optional
from market_data.kline_buffer import KlineBuffer, Kline

logger = logging.getLogger(__name__)


class MarketDataFeed:
    """Binance USDT-M Futures WebSocket 市场数据订阅。

    通过 combined stream 在一个连接中订阅所有标的的 K线 + 标记价格。
    K线闭合时通过 on_kline_closed 回调触发信号引擎。
    """

    def __init__(
        self,
        symbols: List[str],
        testnet: bool = True,
        on_kline_closed: Optional[Callable] = None,
        proxy_host: str = "127.0.0.1",
        proxy_port: int = 7897,
    ):
        self.symbols = symbols
        self.testnet = testnet
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.buffer = KlineBuffer(max_size=500)
        self.on_kline_closed = on_kline_closed or (
            lambda symbol, timeframe, ohlcv: None
        )
        self._mark_prices: Dict[str, float] = {}
        self._last_prices: Dict[str, float] = {}
        self._running = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws = None

    # ─── Stream URL ───

    def _build_stream_url(self) -> str:
        """构建 combined stream URL。

        所有标的和 stream 类型在一个连接中订阅（2026 Binance 新路径规范）:
          - /market/ws 用于 kline, markPrice
          - combined stream: /market/stream?streams=...
        """
        base = "wss://fstream.binance.com/market/stream?streams="
        streams = []
        for sym in self.symbols:
            s = sym.lower()
            streams.extend([
                f"{s}@kline_4h",
                f"{s}@kline_1d",
                f"{s}@kline_1w",
                f"{s}@markPrice@1s",
                f"{s}@aggTrade",
            ])
        return base + "/".join(streams)

    # ─── 时间框架映射 ───

    @staticmethod
    def _timeframe_from_interval(interval: str) -> str:
        mapping = {"1w": "1w", "1d": "1d", "4h": "4h"}
        return mapping.get(interval, interval)

    @staticmethod
    def _stream_timeframe_map(symbols: List[str]) -> Dict[str, str]:
        m = {}
        for sym in symbols:
            s = sym.lower()
            m[f"{s}@kline_4h"] = "4h"
            m[f"{s}@kline_1d"] = "1d"
            m[f"{s}@kline_1w"] = "1w"
        return m

    # ─── 消息处理 ───

    def _on_message(self, raw: str):
        """combined stream 回调入口。"""
        data = json.loads(raw)
        # combined stream 包装: {"stream": "...", "data": {...}}
        inner = data.get("data", data)
        event = inner.get("e", "")
        if event == "kline":
            self._on_kline_message(inner)
        elif event == "markPriceUpdate":
            self._on_mark_price_message(inner)
        elif event == "aggTrade":
            self._on_agg_trade_message(inner)

    def _on_kline_message(self, msg: dict):
        """处理 Kline 事件 → 写入 buffer → 检测闭合 → 触发回调。"""
        k = msg.get("k", {})
        symbol = msg.get("s", "").upper()
        interval = k.get("i", "4h")
        timeframe = self._timeframe_from_interval(interval)

        kline = Kline(
            symbol=symbol,
            timeframe=timeframe,
            open_time=k.get("t", 0),
            close_time=k.get("T", 0),
            open=float(k.get("o", 0)),
            high=float(k.get("h", 0)),
            low=float(k.get("l", 0)),
            close=float(k.get("c", 0)),
            volume=float(k.get("v", 0)),
            is_closed=k.get("x", False),
        )
        prev_closed = self.buffer.is_closed(symbol, timeframe, kline.open_time)
        self.buffer.add(kline)

        if kline.is_closed and not prev_closed:
            ohlcv = self.buffer.get_klines(symbol, timeframe, limit=100)
            self.on_kline_closed(symbol, timeframe, ohlcv)

    def _on_mark_price_message(self, msg: dict):
        """处理标记价格更新。"""
        symbol = msg.get("s", "").upper()
        price = float(msg.get("p", 0))
        self._mark_prices[symbol] = price

    def _on_agg_trade_message(self, msg: dict):
        """处理实时成交价更新（每笔交易推送）。"""
        symbol = msg.get("s", "").upper()
        price = float(msg.get("p", 0))
        self._last_prices[symbol] = price

    def get_mark_price(self, symbol: str) -> Optional[float]:
        """获取某标的的最新标记价格（用于风控/清算判断）。"""
        return self._mark_prices.get(symbol.upper())

    def get_last_price(self, symbol: str) -> Optional[float]:
        """获取某标的的最新实时成交价。"""
        return self._last_prices.get(symbol.upper())

    # ─── 生命周期 ───

    def start(self):
        """启动 WebSocket 连接（后台线程）。"""
        self._running = True
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("MarketDataFeed started")

    def _run(self):
        """WebSocket 主循环（自动重连）。"""
        import websocket as ws_client  # websocket-client 库

        url = self._build_stream_url()
        logger.info(f"Connecting to Binance WS: {url}")

        while self._running and not self._stop.is_set():
            try:
                self._ws = ws_client.WebSocketApp(
                    url,
                    on_message=lambda ws, msg: self._on_message(msg),
                    on_error=lambda ws, e: logger.error(f"WS error: {e}"),
                    on_close=lambda ws, status, msg: logger.info(
                        f"WS closed ({status}): {msg}"
                    ),
                    on_open=lambda ws: logger.info("WS connected"),
                )
                self._ws.run_forever(
                    http_proxy_host=self.proxy_host,
                    http_proxy_port=self.proxy_port,
                    proxy_type="http",
                    ping_interval=30,
                    ping_timeout=10,
                )
            except Exception as e:
                logger.error(f"WS connection failed: {e}")

            if self._running and not self._stop.is_set():
                logger.info("Reconnecting in 5s...")
                self._stop.wait(timeout=5)

    def stop(self):
        """停止 WebSocket 连接。"""
        self._running = False
        self._stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("MarketDataFeed stopped")

"""MarketDataFeed — Binance Futures WebSocket -> Kline buffer -> kline.closed events.

高可用架构：
  4 条并行 WebSocket 连接，通过 Clash round-robin 分发到不同代理节点。
  主连接处理数据，3 条备用连接热备份。
  主连接断开时毫秒级切换，零中断。
"""

import json
import threading
import logging
import time
from collections import OrderedDict
from typing import Callable, Dict, List, Optional
from market_data.kline_buffer import KlineBuffer, Kline
from monitor.collector import MetricsCollector

logger = logging.getLogger(__name__)


class _ConnState:
    """单条 WebSocket 连接的状态跟踪。"""

    __slots__ = ("conn_id", "ws", "thread", "connected", "started_ts", "last_msg_ts")

    def __init__(self, conn_id: int):
        self.conn_id = conn_id
        self.ws = None
        self.thread = None
        self.connected = False
        self.started_ts = 0.0   # 连接建立时间（断连诊断：uptime）
        self.last_msg_ts = 0.0  # 最后一条消息时间（断连诊断：last_msg_ago）


class MarketDataFeed:
    """Binance USDT-M Futures WebSocket 市场数据订阅（高可用版）。

    通过 4 条并行 WebSocket 连接实现零中断故障转移：
      - 主连接处理数据，备用连接热备份
      - 主连接断开时，自动切换到可用备用连接
      - 断开的连接自动重连后补充为新的备用连接

    所有连接通过 Clash 代理（127.0.0.1:7897）路由，
    利用 auto-failover 组的 round-robin 策略分发到不同代理节点。
    """

    def __init__(
        self,
        symbols: List[str],
        testnet: bool = True,
        on_kline_closed: Optional[Callable] = None,
        proxy_host: str = "127.0.0.1",
        proxy_port: int = 7897,
        redundant_connections: int = 4,
        proxy_ports: Optional[List[int]] = None,
    ):
        self.symbols = symbols
        self.testnet = testnet
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.redundant_connections = redundant_connections
        # 每条连接独立端口（按订阅源隔离）；缺省时都用 proxy_port
        self.proxy_ports = proxy_ports or [proxy_port] * redundant_connections
        self.buffer = KlineBuffer(max_size=500)
        self.on_kline_closed = on_kline_closed or (
            lambda symbol, timeframe, ohlcv: None
        )
        self._mark_prices: Dict[str, float] = {}
        self._last_prices: Dict[str, float] = {}
        self._running = False
        self._stop = threading.Event()
        self._conns: List[_ConnState] = []
        self._primary_idx = 0
        self._lock = threading.Lock()
        # 已触发 on_kline_closed 的 (symbol, timeframe, open_time)，LRU 去重。
        # 备用连接也会把闭合 K 线写入 buffer，若用 buffer 的 is_closed 判断，
        # 主连接会因备用先写入而漏触发回调，故此处单独跟踪已通知项。
        self._closed_notified: "OrderedDict" = OrderedDict()
        self._notified_max = 5000
        self._stream_url = self._build_stream_url()

    # ─── Stream URL ───

    def _build_stream_url(self) -> str:
        """构建 combined stream URL（testnet 走 stream.binancefuture.com）。"""
        if self.testnet:
            base = "wss://stream.binancefuture.com/stream?streams="
        else:
            base = "wss://fstream.binance.com/market/stream?streams="
        streams = []
        for sym in self.symbols:
            s = sym.lower()
            streams.extend([
                f"{s}@kline_15m",
                f"{s}@kline_1h",
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
        mapping = {"1w": "1w", "1d": "1d", "4h": "4h", "1h": "1h"}
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

    def _on_message(self, raw: str, notify_closed: bool = True):
        """combined stream 回调入口。notify_closed 仅主连接为 True。"""
        data = json.loads(raw)
        inner = data.get("data", data)
        event = inner.get("e", "")
        if event == "kline":
            self._on_kline_message(inner, notify_closed=notify_closed)
        elif event == "markPriceUpdate":
            self._on_mark_price_message(inner)
        elif event == "aggTrade":
            self._on_agg_trade_message(inner)

    def _on_message_wrapper(self, conn_id: int, raw: str):
        """消息分发：所有连接都处理 kline/markPrice/aggTrade 写入共享 buffer 与
        价格缓存（备用连接始终维持最新数据，切主后无缝、无需回填）；
        仅主连接触发 kline 闭合回调与模块心跳。
        """
        # 每条连接（含备用）都记录最后消息时间，供断连诊断
        if conn_id < len(self._conns):
            self._conns[conn_id].last_msg_ts = time.time()
        is_primary = conn_id == self._primary_idx
        if is_primary:
            # 模块心跳: 主连接有消息到达即视为 feed 存活
            MetricsCollector.instance().heartbeat("market_data")
        self._on_message(raw, notify_closed=is_primary)

    def _on_kline_message(self, msg: dict, notify_closed: bool = True):
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
        self.buffer.add(kline)

        if notify_closed and kline.is_closed:
            key = (symbol, timeframe, kline.open_time)
            with self._lock:
                if key in self._closed_notified:
                    return
                self._closed_notified[key] = None
                self._closed_notified.move_to_end(key)
                if len(self._closed_notified) > self._notified_max:
                    self._closed_notified.popitem(last=False)
            ohlcv = self.buffer.get_klines(symbol, timeframe, limit=100)
            self.on_kline_closed(symbol, timeframe, ohlcv)

    def _on_mark_price_message(self, msg: dict):
        symbol = msg.get("s", "").upper()
        price = float(msg.get("p", 0))
        self._mark_prices[symbol] = price

    def _on_agg_trade_message(self, msg: dict):
        symbol = msg.get("s", "").upper()
        price = float(msg.get("p", 0))
        self._last_prices[symbol] = price

    # ─── 连接状态回调 ───

    def _on_conn_open(self, conn_id: int):
        if conn_id < len(self._conns):
            state = self._conns[conn_id]
            state.connected = True
            state.started_ts = time.time()
        logger.info("Conn %d open (primary=%s)", conn_id, conn_id == self._primary_idx)

    def _on_conn_close(self, conn_id: int, status, msg: str):
        last_msg_ago = -1.0
        uptime = -1.0
        if conn_id < len(self._conns):
            state = self._conns[conn_id]
            state.connected = False
            if getattr(state, "started_ts", 0.0):
                uptime = time.time() - state.started_ts
            if getattr(state, "last_msg_ts", 0.0):
                last_msg_ago = time.time() - state.last_msg_ts
        logger.warning(
            "WS disconnected conn=%d close_code=%s last_msg_ago=%.1fs uptime=%.1fs (msg=%s)",
            conn_id, status, last_msg_ago, uptime, msg,
        )

    def _on_conn_error(self, conn_id: int, error):
        logger.error("Conn %d error: %s", conn_id, error)

    def get_mark_price(self, symbol: str) -> Optional[float]:
        return self._mark_prices.get(symbol.upper())

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self._last_prices.get(symbol.upper())

    # ─── 主连接切换 ───

    def _try_switch_primary(self, failed_idx: int):
        """主连接断开时，切换到下一个可用的备用连接。

        备用连接一直在把消息写入共享 buffer/价格缓存，
        因此切换后数据已就绪，无需额外回填，无缝衔接。
        """
        with self._lock:
            if failed_idx != self._primary_idx:
                return  # 已经不是主连接了，忽略
            for i in range(1, self.redundant_connections):
                idx = (failed_idx + i) % self.redundant_connections
                conn = self._conns[idx]
                if conn.connected:
                    self._primary_idx = idx
                    logger.info(
                        "Switched primary: conn %d -> conn %d",
                        failed_idx, idx,
                    )
                    return
            logger.warning(
                "No available standby for conn %d (all %d down)",
                failed_idx, self.redundant_connections,
            )

    # ─── 历史数据回填 ───

    def backfill(self, limit: int = 100, timeframes: Optional[List[str]] = None):
        """从 REST API 拉取历史 K 线填充 buffer。

        启动时调用，确保信号引擎有足够的历史数据计算指标。
        """
        import requests

        if timeframes is None:
            timeframes = ["15m", "1h", "4h", "1d", "1w"]
        if self.testnet:
            base_url = "https://testnet.binancefuture.com/fapi/v1/klines"
        else:
            base_url = "https://fapi.binance.com/fapi/v1/klines"
        proxies = {"http": f"http://{self.proxy_host}:{self.proxy_port}",
                   "https": f"http://{self.proxy_host}:{self.proxy_port}"}
        for symbol in self.symbols:
            for tf in timeframes:
                try:
                    resp = requests.get(
                        base_url,
                        params={"symbol": symbol, "interval": tf, "limit": limit},
                        proxies=proxies, timeout=10,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    for i, row in enumerate(data):
                        # 最后一条是当前未闭合的 K 线，其余已闭合
                        kline = Kline(
                            symbol=symbol, timeframe=tf,
                            open_time=row[0], close_time=row[6],
                            open=float(row[1]), high=float(row[2]),
                            low=float(row[3]), close=float(row[4]),
                            volume=float(row[5]),
                            is_closed=(i < len(data) - 1),
                        )
                        self.buffer.add(kline)
                    logger.info("Backfilled %s %s: %d klines", symbol, tf, len(data))
                except Exception as e:
                    logger.error("Backfill failed %s %s: %s", symbol, tf, e)

    # ─── 生命周期 ───

    def start(self):
        """启动 4 条 WebSocket 连接（各占一个后台线程）。"""
        if self._running:
            logger.warning("MarketDataFeed already running")
            return
        self._running = True
        self._stop.clear()
        self._conns = []
        self._primary_idx = 0

        for i in range(self.redundant_connections):
            state = _ConnState(i)
            self._conns.append(state)

        for i in range(self.redundant_connections):
            t = threading.Thread(
                target=self._run_conn,
                args=(self._conns[i],),
                daemon=True,
                name=f"ws-conn-{i}",
            )
            t.start()
            self._conns[i].thread = t

        logger.info(
            "MarketDataFeed started (%d connections, primary=0)",
            self.redundant_connections,
        )

    def _run_conn(self, state: _ConnState):
        """单条 WebSocket 连接的主循环（自动重连）。"""
        import websocket as ws_client

        url = self._stream_url
        conn_id = state.conn_id

        while self._running and not self._stop.is_set():
            try:
                ws = ws_client.WebSocketApp(
                    url,
                    on_message=lambda ws, msg: self._on_message_wrapper(conn_id, msg),
                    on_error=lambda ws, e: self._on_conn_error(conn_id, e),
                    on_close=lambda ws, status, msg: self._on_conn_close(
                        conn_id, status, msg
                    ),
                    on_open=lambda ws: self._on_conn_open(conn_id),
                )
                state.ws = ws
                # 每条连接走独立端口（按订阅源隔离）
                conn_port = (
                    self.proxy_ports[conn_id]
                    if conn_id < len(self.proxy_ports)
                    else self.proxy_port
                )
                ws.run_forever(
                    http_proxy_host=self.proxy_host,
                    http_proxy_port=conn_port,
                    proxy_type="http",
                    # 代理延迟可达 6-10s+，ping_timeout 需能容忍波动，避免误判假死。
                    # 约束: ping_interval 必须 > ping_timeout（websocket-client 强制）。
                    ping_interval=60,
                    ping_timeout=30,
                )
            except Exception as e:
                logger.error("Conn %d exception: %s", conn_id, e)

            if self._running and not self._stop.is_set():
                # 连接断开：如果是主连接则尝试切换
                self._try_switch_primary(conn_id)
                self._stop.wait(timeout=1)

    def stop(self):
        """停止所有 WebSocket 连接。"""
        logger.info("MarketDataFeed stopping...")
        self._running = False
        self._stop.set()

        for conn in self._conns:
            if conn.ws:
                try:
                    conn.ws.close()
                except Exception:
                    pass

        for conn in self._conns:
            if conn.thread and conn.thread.is_alive():
                conn.thread.join(timeout=3)

        self._conns.clear()
        logger.info("MarketDataFeed stopped")
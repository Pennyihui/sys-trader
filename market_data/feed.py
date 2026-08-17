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
        archive=None,  # KlineArchive 可选注入 (P2-1 闭合 K 线持久化)
    ):
        self.symbols = symbols
        self.testnet = testnet
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.redundant_connections = redundant_connections
        self.archive = archive
        # 每条连接独立端口（按订阅源隔离）；缺省时都用 proxy_port
        self.proxy_ports = proxy_ports or [proxy_port] * redundant_connections
        self.buffer = KlineBuffer(max_size=500)
        self.on_kline_closed = on_kline_closed or (
            lambda symbol, timeframe, ohlcv: None
        )
        self._mark_prices: Dict[str, float] = {}
        self._last_prices: Dict[str, float] = {}
        # 每 symbol 最后一次行情更新时间（停滞检测用）。
        # 注意: get_last_price 返回缓存价永不为 None，不能作为新鲜度依据——
        # 数据源死亡后缓存价仍在，必须用时间戳判断（2026-08-16 审计修复）。
        self._last_update_ts: Dict[str, float] = {}
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
        # K线闭合 REST 兜底节流: symbol -> 上次 REST 拉取时间 (2026-08-17)
        self._last_closure_poll: Dict[str, float] = {}

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

    # ─── 消息处理 ───

    def _on_message(self, raw: str, notify_closed: bool = True):
        """combined stream 回调入口。notify_closed 仅主连接为 True。

        2026-08-16 审计: 整体 try/except — json 畸形帧/字段缺失不再把异常
        冒泡出 on_message 杀死连接循环 (此前会引发 8 路重连风暴)。
        """
        try:
            data = json.loads(raw)
            inner = data.get("data", data)
            event = inner.get("e", "")
            if event == "kline":
                self._on_kline_message(inner, notify_closed=notify_closed)
            elif event == "markPriceUpdate":
                self._on_mark_price_message(inner)
            elif event == "aggTrade":
                self._on_agg_trade_message(inner)
        except Exception as e:
            logger.error("Feed message parse failed: %s (raw=%.120s)", e, raw)

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
        added = self.buffer.add(kline)
        if not added:
            # 乱序/过期 K 线被丢弃: 不触发闭合回调（该窗口数据不可靠）
            logger.debug(
                "Dropped out-of-order kline %s %s open_time=%s",
                symbol, timeframe, kline.open_time,
            )
            return

        # 闭合 K 线持久化归档 (P2-1), 失败静默 (观测增强, 不阻塞行情)。
        # 仅主连接归档 (2026-08-16 审计: 此前 8 条连接各自 upsert+commit, 8× 写放大)
        if kline.is_closed and self.archive is not None and notify_closed:
            self.archive.upsert(kline)

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
        self._last_update_ts[symbol] = time.time()

    def _on_agg_trade_message(self, msg: dict):
        symbol = msg.get("s", "").upper()
        price = float(msg.get("p", 0))
        self._last_prices[symbol] = price
        self._last_update_ts[symbol] = time.time()

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

    def get_last_update_ts(self, symbol: str) -> Optional[float]:
        """该 symbol 最后一次行情消息的本地时间（epoch 秒），无数据时 None。

        供停滞检测使用：缓存价永不为 None，只有时间戳能反映数据流是否存活。
        """
        return self._last_update_ts.get(symbol.upper())

    _PERIOD_MS = {"1w": 604_800_000, "1d": 86_400_000, "4h": 14_400_000,
                  "1h": 3_600_000, "15m": 900_000}

    def _replay_missed_closures(self):
        """主连接切换后补发窗口期内漏通知的闭合 K 线 (2026-08-16 审计修复)。

        主断→切换窗口 (最坏 ping_timeout 30s) 内备用连接已把闭合 candle 写入
        buffer 但 _closed_notified 未记录; 交易所不重发 → 回调永久丢失。
        只补发最近 2 根周期内的闭合线, 避免把 backfill 的历史线全部重放。
        """
        try:
            now_ms = int(time.time() * 1000)
            for key, klines in self.buffer.all_entries().items():
                for k in klines:
                    if not k.is_closed:
                        continue
                    period = self._PERIOD_MS.get(k.timeframe, 900_000)
                    if now_ms - k.open_time > 2 * period:
                        continue
                    ck = (k.symbol, k.timeframe, k.open_time)
                    with self._lock:
                        if ck in self._closed_notified:
                            continue
                        self._closed_notified[ck] = None
                    logger.warning("REPLAY missed closed kline %s %s open_time=%s",
                                   k.symbol, k.timeframe, k.open_time)
                    ohlcv = self.buffer.get_klines(k.symbol, k.timeframe, limit=100)
                    self.on_kline_closed(k.symbol, k.timeframe, ohlcv)
        except Exception as e:
            logger.error("Replay missed closures failed: %s", e)

    # ─── 主连接切换 ───

    def _try_switch_primary(self, failed_idx: int):
        """主连接断开时，切换到下一个可用的备用连接。

        备用连接一直在把消息写入共享 buffer/价格缓存，
        因此切换后数据已就绪，无需额外回填，无缝衔接。
        """
        switched = False
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
                    switched = True
                    break
            if not switched:
                logger.warning(
                    "No available standby for conn %d (all %d down)",
                    failed_idx, self.redundant_connections,
                )
        # 2026-08-16 修复: 补发逻辑必须在锁外调用 — self._lock 是非重入锁,
        # 此前在 with 块内调用 _replay_missed_closures (其内部又获取同锁)
        # 造成死锁, 卡死后所有连接的 K 线闭合处理永久阻塞 (closes=0)。
        if switched:
            self._replay_missed_closures()

    # ─── 主连接静默断流看护 (2026-08-17 审计) ───

    def primary_stale_seconds(self) -> float:
        """主连接最后一条消息距今秒数; 无消息记录返回 -1 (未知)。"""
        if self._primary_idx >= len(self._conns):
            return -1.0
        state = self._conns[self._primary_idx]
        last = getattr(state, "last_msg_ts", 0.0) or 0.0
        if last <= 0:
            return -1.0
        return time.time() - last

    def force_primary_switch(self):
        """主连接静默断流时强制切主 (2026-08-17 修复)。

        场景: 代理节点半开 — 主连接 TCP 存活、ping 保活正常、on_close 不触发,
        但收不到任何行情消息。此时备用连接仍在喂价格 (stalls=0、ws=8/8 全绿),
        唯独 K 线闭合回调 (仅主连接触发) 永久丢失 → closes 停滞。
        (2026-08-17 24h 实测: closes 停在 168 达 18h, ws=8/8 价格正常)
        主动断开主连接 → on_close → _try_switch_primary → _replay_missed_closures
        补发窗口期漏通知的闭合 K 线。
        """
        idx = self._primary_idx
        if idx >= len(self._conns):
            return
        state = self._conns[idx]
        logger.warning(
            "PRIMARY STALE: 主连接 %d 无消息 %.0fs — 强制断开触发切主+补发",
            idx, self.primary_stale_seconds(),
        )
        ws = getattr(state, "ws", None)
        if ws is not None:
            try:
                ws.close()
            except Exception as e:
                logger.debug("force_primary_switch close exception: %s", e)
        else:
            # ws 对象缺失 (半初始化): 直接标记断开并切主
            self._try_switch_primary(idx)

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

    def poll_closures_from_rest(self):
        """K线闭合 REST 兜底 (2026-08-17): WS kline 流停滞时补触发闭合回调。

        背景: testnet kline stream 曾连续 11h 停止推送 (aggTrade/markPrice
        正常 → ws=8/8、价格正常、stalls=0 全绿, 唯独 closes 不动, 信号链
        静默失明)。watchdog 只告警不处理 — 需要自愈。

        策略: 15m 边界后 ≥60s 用 /fapi/v1/klines 拉最新已闭合 K线,
        若 _closed_notified 未记录则补触发 on_kline_closed (幂等)。
        节流: 每 symbol 每 5 分钟最多拉一次; 只补最近 2 根周期内的闭合
        (防重放历史); 距边界 <60s 跳过 (WS 大概率正常)。
        """
        import requests

        if not self.symbols:
            return
        base_url = ("https://testnet.binancefuture.com/fapi/v1/klines" if self.testnet
                    else "https://fapi.binance.com/fapi/v1/klines")
        proxies = {"http": f"http://{self.proxy_host}:{self.proxy_port}",
                   "https": f"http://{self.proxy_host}:{self.proxy_port}"}
        now_ms = int(time.time() * 1000)
        period = self._PERIOD_MS.get("15m", 900_000)
        if now_ms % period < 60_000:
            return  # 距 15m 边界 <60s: WS 大概率正常, 不打扰
        for symbol in self.symbols:
            last_poll = self._last_closure_poll.get(symbol, 0.0)
            if now_ms - last_poll < 300_000:
                continue  # 每 symbol 5 分钟节流
            self._last_closure_poll[symbol] = now_ms
            try:
                resp = requests.get(
                    base_url,
                    params={"symbol": symbol, "interval": "15m", "limit": 2},
                    proxies=proxies, timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.debug("Closure poll %s failed: %s", symbol, e)
                continue
            if len(data) < 2:
                continue
            row = data[-2]  # 倒数第二根 = 已闭合
            kline = Kline(
                symbol=symbol, timeframe="15m",
                open_time=row[0], close_time=row[6],
                open=float(row[1]), high=float(row[2]),
                low=float(row[3]), close=float(row[4]),
                volume=float(row[5]), is_closed=True,
            )
            if now_ms - kline.open_time > 2 * period:
                continue  # 过期闭合 (历史重放), 跳过
            key = (symbol, "15m", kline.open_time)
            with self._lock:
                if key in self._closed_notified:
                    continue
                self._closed_notified[key] = None
                self._closed_notified.move_to_end(key)
                if len(self._closed_notified) > self._notified_max:
                    self._closed_notified.popitem(last=False)
            self.buffer.add(kline)
            logger.warning(
                "CLOSURE REST FALLBACK: %s 15m open_time=%s 已补触发 "
                "(WS kline 流可能停滞)", symbol, kline.open_time)
            ohlcv = self.buffer.get_klines(symbol, "15m", limit=100)
            self.on_kline_closed(symbol, "15m", ohlcv)

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
                    # 2026-08-16 修复: 部分代理端口连接会悬挂 (TCP 通但节点卡),
                    # 无代理超时则 run_forever 永不返回 → 该连接线程卡死、
                    # 主备切换被跳过 (曾致 ws=3/8 但 closes=0 持续 10h)。
                    # 注意 websocket-client 1.8 的 run_forever 用
                    # http_proxy_timeout 而非 connect_timeout。
                    http_proxy_timeout=20,
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
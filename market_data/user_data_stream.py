"""UserDataStream — Binance Futures 用户数据流 (listenKey WebSocket)。

2026-08-16 补缺: 成交/余额此前靠 10s 轮询, 本模块提供毫秒级推送:
  - ORDER_TRADE_UPDATE  订单成交/撤单/拒绝
  - ACCOUNT_UPDATE      余额/保证金变化 (含资金费结算)
  - listenKeyExpired    需要换 key 重连
  - MARGIN_CALL         保证金率告警

参考: Binance Futures WebSocket 用户数据流 (freqtrade/hummingbot 同款机制),
listenKey 60 分钟有效, 30 分钟保活; testnet 走 stream.binancefuture.com。
"""

import json
import logging
import threading
import time
from typing import Callable, Optional

from execution.order_gateway import OrderGateway

logger = logging.getLogger(__name__)

KEEPALIVE_INTERVAL = 30 * 60   # 30min (listenKey 有效期 60min)
RECONNECT_DELAY = 5            # 重连退避基数 (秒)


class UserDataStream:
    def __init__(
        self,
        gateway: OrderGateway,
        on_order_update: Optional[Callable[[dict], None]] = None,
        on_account_update: Optional[Callable[[dict], None]] = None,
        on_margin_call: Optional[Callable[[dict], None]] = None,
        proxy_host: Optional[str] = None,
        proxy_port: Optional[int] = None,
    ):
        self.gateway = gateway
        self.on_order_update = on_order_update or (lambda d: None)
        self.on_account_update = on_account_update or (lambda d: None)
        self.on_margin_call = on_margin_call or (lambda d: None)
        import os
        self.proxy_host = proxy_host or os.environ.get("PROXY_HOST", "127.0.0.1")
        self.proxy_port = proxy_port or int(os.environ.get("PROXY_PORT", "7897"))
        self._listen_key: Optional[str] = None
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._started = False

    @property
    def ws_url(self) -> str:
        # 2026-08-16 审计: 锁内读 key, 防保活线程刷新 key 与 WS 建连竞态
        with self._lock:
            key = self._listen_key or "pending"
        base = ("wss://stream.binancefuture.com/ws"
                if self.gateway.testnet else "wss://fstream.binance.com/ws")
        return f"{base}/{key}"

    def _ensure_listen_key(self) -> bool:
        with self._lock:
            if self._listen_key:
                return True
            self._listen_key = self.gateway.create_listen_key()
            return bool(self._listen_key)

    def _refresh_listen_key(self):
        """listenKeyExpired 或保活失败后换新 key (旧 key 24h 内自动失效)。"""
        with self._lock:
            self._listen_key = None
        logger.warning("User data stream: 更换 listenKey 并重连")
        self._ensure_listen_key()

    def _on_message(self, raw: str):
        try:
            data = json.loads(raw)
        except ValueError:
            logger.warning("User data stream: 非 JSON 消息: %.200s", raw)
            return
        event = data.get("e", "")
        if event == "ORDER_TRADE_UPDATE":
            self.on_order_update(data.get("o", {}))
        elif event == "ACCOUNT_UPDATE":
            self.on_account_update(data.get("a", {}))
        elif event == "MARGIN_CALL":
            self.on_margin_call(data)
        elif event == "listenKeyExpired":
            logger.warning("User data stream: listenKeyExpired")
            self._refresh_listen_key()
            # 2026-08-16: 换 key 后主动断旧连接, 强制用新 key 重连
            # (原实现旧 WS 静默失效, 靠 10s 轮询兜底才发现)
            try:
                with self._lock:
                    ws = self._ws
                if ws:
                    ws.close()
            except Exception:
                pass
        elif event == "ACCOUNT_CONFIG_UPDATE":
            logger.info("User data stream: 账户配置变更 %s", data.get("ac", {}))
        # 其余事件 (听不到/心跳) 忽略

    def _run_ws(self):
        import websocket as ws_client

        while self._started and not self._stop.is_set():
            if not self._ensure_listen_key():
                logger.error("User data stream: 获取 listenKey 失败, %ds 后重试",
                             RECONNECT_DELAY)
                self._stop.wait(timeout=RECONNECT_DELAY)
                continue
            try:
                ws = ws_client.WebSocketApp(
                    self.ws_url,
                    on_message=lambda ws, msg: self._on_message(msg),
                    on_error=lambda ws, e: logger.error("User stream WS error: %s", e),
                    on_close=lambda ws, status, msg: logger.warning(
                        "User stream WS closed code=%s msg=%s", status, msg),
                    on_open=lambda ws: logger.info("User data stream connected"),
                )
                with self._lock:
                    self._ws = ws
                ws.run_forever(
                    http_proxy_host=self.proxy_host,
                    http_proxy_port=self.proxy_port,
                    proxy_type="http",
                    ping_interval=60,
                    ping_timeout=30,
                )
            except Exception as e:
                logger.error("User stream exception: %s", e)
            if self._started and not self._stop.is_set():
                self._stop.wait(timeout=RECONNECT_DELAY)

    def _keepalive_loop(self):
        while not self._stop.is_set():
            self._stop.wait(timeout=KEEPALIVE_INTERVAL)
            if self._stop.is_set():
                break
            with self._lock:
                key = self._listen_key
            if not self.gateway.keepalive_listen_key(key or ""):
                logger.warning("User stream keepalive 失败 → 换 key")
                self._refresh_listen_key()

    def start(self):
        if self._started:
            return
        self._started = True
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_ws, daemon=True,
                                        name="user-data-stream")
        self._thread.start()
        self._keepalive_thread = threading.Thread(
            target=self._keepalive_loop, daemon=True, name="user-stream-keepalive")
        self._keepalive_thread.start()
        logger.info("UserDataStream started")

    def stop(self):
        if not self._started:
            return
        logger.info("UserDataStream stopping...")
        self._started = False
        self._stop.set()
        with self._lock:
            ws = self._ws
            self._ws = None
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        for t in (self._thread, self._keepalive_thread):
            if t and t.is_alive():
                t.join(timeout=3)
        logger.info("UserDataStream stopped")

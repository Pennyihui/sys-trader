"""ForceOrderStream — 独立 WS 监听 <sym>@forceOrder 强平流 (2026-08-16 #7)。

强平事件是市场级推送 (非用户数据流), 大额强平通常是流动性踩踏前兆。
独立连接 + 独立故障域: testnet 或某代理端口不支持该流时只影响本监听,
绝不拖垮主 8 路行情连接 (主 feed 每路都承载 K线/价格, 风险不对称)。

启用: FORCE_ORDER_STREAM=1 (默认关闭)。
"""

import json
import logging
import threading
import time
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class ForceOrderStream:
    def __init__(self, symbols: List[str], testnet: bool = True,
                 proxy_host: str = "127.0.0.1", proxy_port: int = 7897,
                 on_force_order: Optional[Callable[[dict], None]] = None):
        self.symbols = symbols
        self.testnet = testnet
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.on_force_order = on_force_order or (lambda data: None)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _stream_url(self) -> str:
        if self.testnet:
            base = "wss://stream.binancefuture.com/stream?streams="
        else:
            base = "wss://fstream.binance.com/market/stream?streams="
        return base + "/".join(f"{s.lower()}@forceOrder" for s in self.symbols)

    def _on_message(self, raw: str):
        try:
            data = json.loads(raw)
            inner = data.get("data", data)
            if inner.get("e") == "forceOrder":
                self.on_force_order(inner)
        except Exception as e:
            logger.debug("forceOrder 消息解析失败: %s", e)

    def _run(self):
        import websocket as ws_client

        url = self._stream_url()
        while not self._stop.is_set():
            try:
                ws = ws_client.WebSocketApp(
                    url,
                    on_message=lambda ws, msg: self._on_message(msg),
                    on_error=lambda ws, e: logger.debug(
                        "forceOrder WS error: %s", e),
                    on_close=lambda ws, s, m: logger.info(
                        "forceOrder WS closed (%s %s)", s, m),
                )
                ws.run_forever(
                    http_proxy_host=self.proxy_host,
                    http_proxy_port=self.proxy_port,
                    proxy_type="http",
                    ping_interval=60,
                    ping_timeout=30,
                    http_proxy_timeout=20,
                )
            except Exception as e:
                logger.debug("forceOrder WS 异常: %s", e)
            # 断开后 30s 重连 (该流低频, 无需秒级重试)
            self._stop.wait(timeout=30)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("ForceOrderStream started (%d symbols)", len(self.symbols))

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

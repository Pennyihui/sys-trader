"""FastAPI WebSocket 服务 — 实时推送交易系统数据到 Dashboard。"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional, Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from dashboard.data_collector import DataCollector

logger = logging.getLogger(__name__)

PROXY_POOL_API = "http://127.0.0.1:8765"


def handle_ws_command(event_bus, msg: str):
    """dashboard 命令 → command 事件流（kill switch 接线）。"""
    if event_bus is not None:
        event_bus.publish("command", {"command": msg})


class DashboardServer:
    def __init__(self, data_collector: DataCollector, push_interval: float = 1.0,
                 event_bus=None):
        self.collector = data_collector
        self.push_interval = push_interval
        self.event_bus = event_bus
        self._app: Optional[FastAPI] = None
        self._clients: Set[WebSocket] = set()

    def _create_app(self) -> FastAPI:
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            task = asyncio.create_task(self._broadcast_loop())
            yield
            task.cancel()

        app = FastAPI(lifespan=lifespan)

        @app.websocket("/ws")
        async def websocket_endpoint(ws: WebSocket):
            await ws.accept()
            self._clients.add(ws)
            try:
                while True:
                    msg = await ws.receive_text()
                    if msg in ("pause", "resume", "emergency_stop"):
                        logger.info("[Dashboard] command: %s", msg)
                        handle_ws_command(self.event_bus, msg)
            except WebSocketDisconnect:
                pass
            finally:
                self._clients.discard(ws)

        @app.get("/health")
        async def health():
            return {"status": "ok", "clients": len(self._clients)}

        @app.get("/api/proxy-pool")
        async def proxy_pool_status():
            """返回代理池状态（透传 Proxy Pool Service）。"""
            import json, urllib.request
            try:
                req = urllib.request.Request(
                    f"{PROXY_POOL_API}/status",
                    headers={"User-Agent": "Dashboard/1.0"},
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                return {"status": "unavailable", "message": str(e)}

        return app

    async def _broadcast_loop(self):
        while True:
            await asyncio.sleep(self.push_interval)
            data = self.collector.collect()
            dead: Set[WebSocket] = set()
            for ws in self._clients:
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.add(ws)
            self._clients -= dead

    @property
    def app(self) -> FastAPI:
        if self._app is None:
            self._app = self._create_app()
        return self._app

    def run(self, host: str = "0.0.0.0", port: int = 8000):
        uvicorn.run(self.app, host=host, port=port)


def create_app(data_collector: Optional[DataCollector] = None, event_bus=None) -> FastAPI:
    """工厂函数：创建 DashboardServer 实例并返回 FastAPI app。

    未提供 data_collector 时自动装配：EventBus（Redis）→ StateStore（消费
    position/order/signal/heartbeat 流）→ MarketDataFeed（Binance WS 行情），
    供生产 uvicorn 入口使用。
    """
    if data_collector is None:
        from shared.event_bus import EventBus
        from dashboard.state_store import StateStore
        from market_data.feed import MarketDataFeed
        from shared.config_loader import load_env
        load_env()
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        if event_bus is None:
            event_bus = EventBus(redis_url=redis_url)
        symbols = [s.strip() for s in os.environ.get("DASHBOARD_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
        feed = MarketDataFeed(
            symbols=symbols,
            proxy_host=os.environ.get("PROXY_HOST", "127.0.0.1"),
            proxy_port=int(os.environ.get("PROXY_PORT", "7897")),
        )
        store = StateStore(event_bus=event_bus, instance_filter=os.environ.get("DASHBOARD_INSTANCE", "live"))
        try:
            store.start()
        except Exception as e:
            logger.warning("StateStore start failed (Redis down?): %s", e)
        feed.start()
        collector = DataCollector(state_store=store, feed=feed)
    else:
        collector = data_collector
    return DashboardServer(data_collector=collector, event_bus=event_bus).app


# uvicorn 入口 (python -m uvicorn dashboard.server:app)
app = create_app()

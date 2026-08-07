"""FastAPI WebSocket 服务 — 实时推送交易系统数据到 Dashboard。"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from dashboard.data_collector import DataCollector

logger = logging.getLogger(__name__)

PROXY_POOL_API = "http://127.0.0.1:8765"


class DashboardServer:
    def __init__(self, data_collector: DataCollector, push_interval: float = 1.0):
        self.collector = data_collector
        self.push_interval = push_interval
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


def create_app(data_collector: Optional[DataCollector] = None) -> FastAPI:
    """工厂函数：创建 DashboardServer 实例并返回 FastAPI app。

    未提供 data_collector 时创建空实例供测试/开发使用。
    """
    if data_collector is None:
        from portfolio.tracker import PortfolioTracker
        from market_data.feed import MarketDataFeed
        feed = MarketDataFeed(symbols=[], proxy_host="127.0.0.1", proxy_port=7897)
        collector = DataCollector(feed=feed, portfolio=PortfolioTracker())
    else:
        collector = data_collector
    return DashboardServer(data_collector=collector).app


# uvicorn 入口 (python -m uvicorn dashboard.server:app)
app = create_app()

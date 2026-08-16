"""FastAPI WebSocket 服务 — 实时推送交易系统数据到 Dashboard。"""

import asyncio
import csv
import json
import logging
import os
import sys
import time
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional, Set

# 支持 `python dashboard/server.py` / PM2 直接运行: 项目根入 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from dashboard.data_collector import DataCollector

logger = logging.getLogger(__name__)

PROXY_POOL_API = "http://127.0.0.1:8765"
NETWORK_MONITOR_API = "http://127.0.0.1:8766"
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 外部服务状态 TTL 缓存 (运维看板用, 10s)
_EXT_CACHE: dict = {}
_EXT_CACHE_TS: dict = {}
_EXT_TTL = 10.0


def _cached_external(name: str, fetcher):
    now = time.time()
    if now - _EXT_CACHE_TS.get(name, 0.0) < _EXT_TTL and name in _EXT_CACHE:
        return _EXT_CACHE[name]
    try:
        value = fetcher()
    except Exception:
        value = None
    _EXT_CACHE[name] = value
    _EXT_CACHE_TS[name] = now
    return value


def _fetch_json(url: str, timeout: float = 3.0):
    req = urllib.request.Request(url, headers={"User-Agent": "Dashboard/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _soak_metrics(limit: int = 500):
    """读取 soak_watchdog 的 RSS/CPU/错误增量 CSV (历史运维数据)。"""
    path = PROJECT_ROOT / "logs" / "soak_metrics.csv"
    if not path.exists():
        return {"rows": [], "total_errors": 0}
    rows = []
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({
                    "ts": int(float(r.get("ts", 0))),
                    "rss_mb": float(r.get("rss_mb", 0) or 0),
                    "cpu_pct": float(r.get("cpu_pct", 0) or 0),
                    "errors_delta": int(r.get("errors_delta", 0) or 0),
                })
    except Exception as e:
        logger.error("soak_metrics read failed: %s", e)
        return {"rows": [], "total_errors": 0}
    rows = rows[-limit:]
    return {"rows": rows, "total_errors": sum(r["errors_delta"] for r in rows)}


def _logs_size_mb() -> float:
    """logs/ 目录体积 (MB) — 磁盘占用运维卡。"""
    try:
        log_dir = PROJECT_ROOT / "logs"
        if not log_dir.exists():
            return 0.0
        return round(sum(f.stat().st_size for f in log_dir.glob("*") if f.is_file()) / 1024 / 1024, 1)
    except Exception:
        return 0.0


_KLINE_PERIOD_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000,
                    "1d": 86_400_000, "1w": 604_800_000}


def _seed_kline_archive(db_path, symbol: str, timeframe: str, limit: int = 500):
    """归档蜡烛不足/过期时从币安 REST 拉历史补齐 (2026-08-16)。

    数据源默认实盘公开 K 线 (KLINE_DATA_SOURCE=live): testnet 的高周期
    历史数据冻结 (实测 1d 停在 4 天前、1w 停在 2.5 个月前), 图表会出现
    断裂; 实盘公开接口历史完整且实时, 仅用于图表展示。
    判定: 归档数量 < limit, 或最新蜡烛 open_time 距今 > 2 个周期 → 全量重灌。
    """
    import sqlite3
    import requests as _requests
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT COUNT(*), MAX(open_time) FROM klines "
                "WHERE symbol=? AND timeframe=?",
                (symbol, timeframe)).fetchone()
            cnt, max_ot = row[0], row[1]
        finally:
            conn.close()
        period = _KLINE_PERIOD_MS.get(timeframe, 900_000)
        now_ms = int(time.time() * 1000)
        stale = max_ot is not None and (now_ms - int(max_ot)) > 2 * period
        if cnt >= limit and not stale:
            return
        source = os.environ.get("KLINE_DATA_SOURCE", "live")
        base = ("https://testnet.binancefuture.com" if source == "testnet"
                else "https://fapi.binance.com")
        proxy_host = os.environ.get("PROXY_HOST", "127.0.0.1")
        proxy_port = int(os.environ.get("PROXY_PORT", "7897"))
        proxies = {"http": f"http://{proxy_host}:{proxy_port}",
                   "https": f"http://{proxy_host}:{proxy_port}"}
        resp = _requests.get(f"{base}/fapi/v1/klines",
                             params={"symbol": symbol, "interval": timeframe,
                                     "limit": limit},
                             proxies=proxies, timeout=15)
        resp.raise_for_status()
        rows = resp.json()
        conn = sqlite3.connect(str(db_path))
        try:
            if stale:
                # 过期数据全量重灌 (testnet 高周期冻结修复)
                conn.execute(
                    "DELETE FROM klines WHERE symbol=? AND timeframe=?",
                    (symbol, timeframe))
            # 最后一行是未闭合 K 线, 不归档 (与 feed 语义一致)
            for i, row in enumerate(rows[:-1]):
                conn.execute(
                    """INSERT OR REPLACE INTO klines
                       (symbol, timeframe, open_time, close_time, open, high,
                        low, close, volume)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (symbol, timeframe, int(row[0]), int(row[6]),
                     float(row[1]), float(row[2]), float(row[3]),
                     float(row[4]), float(row[5])))
            conn.commit()
        finally:
            conn.close()
        logger.info("K线归档补种 %s %s: +%d 根历史蜡烛 (source=%s%s)",
                    symbol, timeframe, len(rows) - 1, source,
                    ", 过期重灌" if stale else "")
    except Exception as e:
        logger.warning("K线归档补种失败 %s %s: %s", symbol, timeframe, e)


def _klines(symbol: str, timeframe: str = "15m", limit: int = 500) -> dict:
    """从 K线归档 (data/kline.db) 读取蜡烛图数据 (不足时自动补种历史)。"""
    import sqlite3
    db_path = PROJECT_ROOT / "data" / "kline.db"
    if not db_path.exists():
        return {"symbol": symbol, "timeframe": timeframe, "candles": []}
    _seed_kline_archive(db_path, symbol, timeframe, limit=limit)
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT open_time, open, high, low, close, volume FROM klines
               WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT ?""",
            (symbol, timeframe, limit)).fetchall()
        conn.close()
        candles = [{
            "open_time": r["open_time"],
            "open": r["open"], "high": r["high"], "low": r["low"],
            "close": r["close"], "volume": r["volume"],
        } for r in reversed(rows)]
        return {"symbol": symbol, "timeframe": timeframe, "candles": candles}
    except Exception as e:
        logger.error("kline read failed: %s", e)
        return {"symbol": symbol, "timeframe": timeframe, "candles": []}


def handle_ws_command(event_bus, msg) -> bool:
    """dashboard 命令 → command 事件流（kill switch / 手动平仓等接线）。

    msg 可为字符串命令 ("pause"/"resume"/"emergency_stop") 或
    带参数的命令字典 ({"command": "force_exit", "symbol": "BTCUSDT"})。
    返回是否成功发布。
    """
    if event_bus is None:
        return False
    data = {"command": msg} if isinstance(msg, str) else dict(msg)
    return bool(event_bus.publish("command", data))


class DashboardServer:
    def __init__(self, data_collector: DataCollector, push_interval: float = 1.0,
                 event_bus=None, ops_archive=None):
        self.collector = data_collector
        self.push_interval = push_interval
        self.event_bus = event_bus
        self.ops_archive = ops_archive  # OpsArchive (运维历史, 可 None)
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
            # 命令通道鉴权 (2026-08-16 审计): DASHBOARD_TOKEN 配置时,
            # 未携带 ?token= 或 token 不匹配的连接直接拒绝 (4401)
            expected_token = os.environ.get("DASHBOARD_TOKEN", "").strip()
            if expected_token:
                provided = (ws.query_params.get("token") or "").strip()
                if provided != expected_token:
                    await ws.close(code=4401)
                    logger.warning("WS 连接被拒绝: token 不匹配")
                    return
            await ws.accept()
            self._clients.add(ws)
            try:
                while True:
                    msg = await ws.receive_text()
                    # 支持: pause / resume / emergency_stop /
                    #       force_exit:<SYMBOL|ALL> / cancel_all:<SYMBOL|ALL> /
                    #       JSON {"command": "setparam", "key": ..., "value": ...}
                    if msg.startswith("{"):
                        try:
                            import json as _json
                            payload = _json.loads(msg)
                            command = payload.get("command", "")
                            ok = handle_ws_command(self.event_bus, payload)
                            logger.info("[Dashboard] command: %s %s", command,
                                        payload.get("symbol") or payload.get("key") or "")
                        except ValueError:
                            continue
                    else:
                        command, _, payload = msg.partition(":")
                        if command in ("pause", "resume", "emergency_stop"):
                            logger.info("[Dashboard] command: %s", command)
                            ok = handle_ws_command(self.event_bus, msg)
                        elif command in ("force_exit", "cancel_all"):
                            symbol = payload.strip().upper()
                            logger.info("[Dashboard] command: %s %s", command, symbol or "ALL")
                            ok = handle_ws_command(
                                self.event_bus,
                                {"command": command, "symbol": symbol or "ALL"},
                            )
                        else:
                            continue
                    await ws.send_json({"type": "command_ack", "command": command,
                                        "ok": bool(ok),
                                        "error": "" if ok else "publish failed (Redis down?)"})
            except WebSocketDisconnect:
                pass
            finally:
                self._clients.discard(ws)

        @app.get("/health")
        async def health():
            return {"status": "ok", "clients": len(self._clients)}

        @app.get("/metrics")
        async def metrics():
            """指标导出 (P2-4): MetricsCollector 心跳/计数器/仪表值快照。"""
            from monitor.collector import MetricsCollector
            return MetricsCollector.instance().snapshot()

        # ─── 运维看板 API (2026-08-16) ───

        @app.get("/api/ops/summary")
        async def ops_summary():
            """运维摘要: 最新心跳统计 + 代理池/网络状态 + 系统运行时长。"""
            latest = self.ops_archive.latest() if self.ops_archive else None
            proxy = _cached_external(
                "proxy_pool", lambda: _fetch_json(f"{PROXY_POOL_API}/status"))
            network = _cached_external(
                "network", lambda: _fetch_json(f"{NETWORK_MONITOR_API}/status"))
            now = time.time()
            return {
                "heartbeat": latest,
                "uptime_seconds": round(now - latest["ts"], 0) if latest else None,
                "proxy_pool": proxy or {"status": "unavailable", "total": 0,
                                        "healthy": 0, "unhealthy": 0},
                "network": network or {"status": "unavailable", "latest": {},
                                       "stats_1h": {}, "stats_24h": {}},
                "log_size_mb": _logs_size_mb(),
            }

        @app.get("/api/ops/history")
        async def ops_history(hours: int = 24):
            """心跳历史曲线数据 (kline闭合/订单/时间偏移/模块心跳)。"""
            if self.ops_archive is None:
                return {"points": []}
            return {"points": self.ops_archive.history(hours=hours)}

        @app.get("/api/ops/commands")
        async def ops_commands(limit: int = 100):
            """运维命令事件时间线 (pause/resume/emergency_stop/...)。"""
            if self.ops_archive is None:
                return {"commands": []}
            return {"commands": self.ops_archive.commands(limit=limit)}

        @app.get("/api/ops/soak")
        async def ops_soak():
            """soak 指标历史: 每小时 RSS/CPU + 错误增量 (soak_metrics.csv)。"""
            return _soak_metrics()

        @app.get("/api/ops/equity")
        async def ops_equity(hours: int = 24):
            """权益曲线 (equity_history 归档)。"""
            if self.ops_archive is None:
                return {"points": []}
            return {"points": self.ops_archive.equity(hours=hours)}

        @app.get("/api/ops/trades")
        async def ops_trades(limit: int = 100):
            """平仓交易明细 (trade_history 归档)。"""
            if self.ops_archive is None:
                return {"trades": []}
            # 2026-08-16 审计: clamp 负值, 防无界查询
            return {"trades": self.ops_archive.trades(limit=max(1, min(limit, 1000)))}

        @app.get("/api/ops/alerts")
        async def ops_alerts(limit: int = 100):
            """告警历史时间线 (钉钉/看门狗告警归档)。"""
            if self.ops_archive is None:
                return {"alerts": []}
            return {"alerts": self.ops_archive.alerts(limit=max(1, min(limit, 1000)))}

        @app.get("/api/ops/restarts")
        async def ops_restarts(limit: int = 50):
            """进程启动/停止历史。"""
            if self.ops_archive is None:
                return {"restarts": []}
            return {"restarts": self.ops_archive.restarts(limit=max(1, min(limit, 1000)))}

        @app.get("/api/kline")
        async def kline(symbol: str = "BTCUSDT", timeframe: str = "15m",
                         limit: int = 500):
            """K线蜡烛数据 (K线归档, 不足时自动补种 500 根历史)。"""
            tf = timeframe if timeframe in ("15m", "1h", "4h", "1d", "1w") else "15m"
            return _klines(symbol.upper(), tf, max(50, min(limit, 1000)))

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
            try:
                data = self.collector.collect()
            except Exception as e:
                logger.error("DataCollector.collect failed: %s", e)
                continue  # collect 异常不再杀死广播任务 (2026-08-16 审计)
            dead: Set[WebSocket] = set()
            for ws in list(self._clients):
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


def create_app(data_collector: Optional[DataCollector] = None, event_bus=None,
               ops_archive=None) -> FastAPI:
    """工厂函数：创建 DashboardServer 实例并返回 FastAPI app。

    未提供 data_collector 时自动装配：EventBus（Redis）→ StateStore（消费
    position/order/signal/heartbeat 流）→ OpsArchive（运维历史归档）→
    MarketDataFeed（Binance WS 行情），供生产 uvicorn 入口使用。
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
        if ops_archive is None:
            try:
                from dashboard import ops_archive as ops_archive_mod
                ops_archive = ops_archive_mod.OpsArchive(
                    db_path=os.environ.get(
                        "OPS_HISTORY_DB", ops_archive_mod.DEFAULT_DB_PATH),
                    retention_days=int(os.environ.get("OPS_HISTORY_DAYS", "7")),
                )
                ops_archive.start(event_bus)
            except Exception as e:
                logger.warning("OpsArchive start failed (Redis down?): %s", e)
                ops_archive = None
        feed.start()
        collector = DataCollector(state_store=store, feed=feed)
    else:
        collector = data_collector
    return DashboardServer(data_collector=collector, event_bus=event_bus,
                           ops_archive=ops_archive).app


# uvicorn 入口 (python -m uvicorn dashboard.server:app)
# 2026-08-16 审计: 模块级装配会在 import 时真连 Redis + 起 4 条真实 WS 行情线程,
# 单测 import 本模块即触发。DASHBOARD_AUTOSTART=0 时惰性化 (conftest 设置),
# 生产/uvicorn 默认 1 不变。
if os.environ.get("DASHBOARD_AUTOSTART", "1") != "0":
    app = create_app()
else:
    app = None


if __name__ == "__main__":
    # PM2/直接运行入口: python dashboard/server.py 等价于 uvicorn 启动。
    # (2026-08-16 审计: 此前无 __main__ 块, PM2 启动后进程立即退出并循环重启)
    import argparse
    parser = argparse.ArgumentParser(description="Dashboard server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)

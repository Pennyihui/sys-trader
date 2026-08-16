"""DataCollector — 从各模块聚合数据，供 Dashboard WebSocket 推送。"""

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional
from market_data.feed import MarketDataFeed

logger = logging.getLogger(__name__)

# Proxy Pool Service API 地址
PROXY_POOL_API = "http://127.0.0.1:8765"
# Network Monitor API 地址
NETWORK_MONITOR_API = "http://127.0.0.1:8766"

_TICKER_CACHE: Dict[str, Any] = {}
_TICKER_CACHE_TS = 0.0
# 2026-08-16: 60s → 10s — 用户反馈行情条"不动"; 失败时保留旧缓存兜底,
# 但正常路径 10s 刷新一次 (3 个交易对的公开 ticker, 负载可忽略)
_TICKER_TTL = 10.0


class DataCollector:
    def __init__(self, state_store, feed: MarketDataFeed):
        self.state = state_store
        self.feed = feed
        # 外部服务状态 TTL 缓存: 同步 HTTP 调用会阻塞 dashboard 事件循环
        # (各 3s 超时, 最坏 6s), 10s 内复用缓存 (2026-08-16 审计)。
        self._cache: Dict[str, Any] = {}
        self._cache_ts: Dict[str, float] = {}
        self._cache_ttl = 10.0

    def _cached(self, name: str, fetcher) -> Any:
        now = time.time()
        if now - self._cache_ts.get(name, 0.0) < self._cache_ttl:
            return self._cache.get(name)
        try:
            value = fetcher()
        except Exception:
            value = None
        self._cache[name] = value
        self._cache_ts[name] = now
        return value

    def collect(self) -> Dict[str, Any]:
        # 锁内快照 (StateStore 消费线程并发写, 直接迭代有 RuntimeError 竞态);
        # MagicMock/旧 StateStore 无该方法时退化 (测试兼容)
        positions_map = {}
        snapshot_fn = getattr(self.state, "positions_snapshot", None)
        if callable(snapshot_fn):
            snap = snapshot_fn()
            if isinstance(snap, dict):
                positions_map = snap
        if not positions_map:
            try:
                positions_map = dict(getattr(self.state, "positions", {}) or {})
            except Exception:
                positions_map = {}
        positions = []
        # 实际往返费率 (2026-08-16 #1): 优先 StateStore 收到的 equity.fee_rate,
        # 其次 FEE_RATE 环境变量, 最后 0.001 默认 — 保本价与盈亏口径同源
        fee_rate = getattr(self.state, "fee_rate", None)
        if not isinstance(fee_rate, (int, float)) or fee_rate <= 0:
            try:
                fee_rate = float(os.environ.get("FEE_RATE", "0.001"))
            except (TypeError, ValueError):
                fee_rate = 0.001
        risk_map = getattr(self.state, "position_risks", None)
        risk_map = risk_map if isinstance(risk_map, dict) else {}
        for symbol, pos in positions_map.items():
            mark = self.feed.get_mark_price(symbol) or pos.get("mark_price") or 0.0
            # 生产 payload（position.changed open）不含 unrealized_pnl，
            # 有行情时实时计算；无行情时回退 payload 值（保持测试兼容）。
            entry = pos.get("entry_price") or 0.0
            qty = pos.get("quantity") or 0.0
            direction = pos.get("direction")
            direction_mult = 1 if direction == "LONG" else -1
            upnl = ((mark - entry) * qty * direction_mult
                    if entry and mark else pos.get("unrealized_pnl", 0.0))
            # 盈亏平衡价 (2026-08-16 #10): 往返 taker 手续费折入价格,
            # LONG: entry·(1+f)/(1-f), SHORT: entry·(1-f)/(1+f)
            if entry and direction in ("LONG", "SHORT") and fee_rate > 0:
                break_even = (entry * (1 + fee_rate) / (1 - fee_rate)
                              if direction == "LONG"
                              else entry * (1 - fee_rate) / (1 + fee_rate))
            else:
                break_even = entry
            # 清算价/爆仓距离/ADL (2026-08-16 #2): runner position.risk 事件
            risk = risk_map.get(symbol) or {}
            liq = risk.get("liquidation_price") or 0.0
            liq_dist = risk.get("liq_distance_pct")
            if liq_dist is None and liq and mark:
                liq_dist = abs(mark - liq) / mark
            positions.append({
                "symbol": symbol,
                "direction": direction,
                "quantity": pos.get("quantity"),
                "entry_price": pos.get("entry_price"),
                "break_even": round(break_even, 6) if break_even else 0.0,
                "mark_price": round(mark, 2),
                "unrealized_pnl": round(upnl, 2),
                "liquidation_price": round(liq, 2) if liq else None,
                "liq_distance_pct": round(liq_dist, 4) if liq_dist is not None else None,
                "adl_quantile": risk.get("adl_quantile"),
            })
        assets = getattr(self.state, "assets", None)
        assets = assets if isinstance(assets, list) else []
        avail = getattr(self.state, "available_balance", 0.0)
        avail = avail if isinstance(avail, (int, float)) else 0.0
        return {
            "equity": round(self.state.equity, 2),
            "margin_ratio": round(self.state.margin_ratio, 2),
            "daily_pnl": round(self.state.daily_pnl, 2),
            "drawdown": round(self.state.drawdown, 4),
            "position_count": len(positions),
            "positions": positions,
            "assets": assets,
            "available_balance": round(avail, 2),
            "tickers": self._collect_tickers(),
            # 行情条最近成功更新时间 (前端显示"x秒前更新")
            "tickers_updated_at": _TICKER_CACHE_TS,
            "signals": getattr(self.state, "signals", []),
            "orders": getattr(self.state, "orders", []),
            "heartbeats": getattr(self.state, "heartbeats", {}),
            "prices": self._collect_prices(positions_map),
            "proxy_pool": self._collect_proxy_pool(),
            "network": self._collect_network(),
        }

    def _collect_prices(self, positions_map: Optional[dict] = None) -> Dict:
        prices = {}
        for symbol in (positions_map or {}).keys():
            last = self.feed.get_last_price(symbol)
            mark = self.feed.get_mark_price(symbol)
            if last or mark:
                prices[symbol] = {"last": last, "mark": mark}
        return prices

    def _collect_tickers(self) -> list:
        """24h 行情条 (公开 ticker, TTL 60s 缓存, 面板二期 2026-08-16)。

        只返回 DASHBOARD_SYMBOLS 配置的交易对。2026-08-16 修复: ① URL 编码
        改用 quote (quote_plus 把空格变 '+' 导致币安忽略 symbols 参数回退
        全市场); ② 响应侧再按白名单过滤, 双保险防全市场刷屏。
        """
        global _TICKER_CACHE, _TICKER_CACHE_TS
        now = time.time()
        if _TICKER_CACHE and now - _TICKER_CACHE_TS < _TICKER_TTL:
            return _TICKER_CACHE
        symbols = [s.strip().upper() for s in
                   os.environ.get("DASHBOARD_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
                   if s.strip()]
        if not symbols:
            return []
        try:
            base = ("https://testnet.binancefuture.com"
                    if os.environ.get("DASHBOARD_TESTNET", "1") == "1"
                    else "https://fapi.binance.com")
            proxy_host = os.environ.get("PROXY_HOST", "127.0.0.1")
            proxy_port = os.environ.get("PROXY_PORT", "7897")
            proxies = {"http": f"http://{proxy_host}:{proxy_port}",
                       "https": f"http://{proxy_host}:{proxy_port}"}
            qs = urllib.parse.urlencode(
                {"symbols": json.dumps(symbols)},
                quote_via=urllib.parse.quote)
            req = urllib.request.Request(
                f"{base}/fapi/v1/ticker/24hr?{qs}",
                headers={"User-Agent": "DataCollector/1.0"})
            proxy_handler = urllib.request.ProxyHandler(proxies)
            opener = urllib.request.build_opener(proxy_handler)
            with opener.open(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            wanted = set(symbols)
            # 双保险: 即使接口回退全市场, 也只保留白名单交易对
            tickers = [{
                "symbol": t.get("symbol", ""),
                "last": float(t.get("lastPrice", 0) or 0),
                "change_pct": float(t.get("priceChangePercent", 0) or 0),
                "high": float(t.get("highPrice", 0) or 0),
                "low": float(t.get("lowPrice", 0) or 0),
            } for t in data if t.get("symbol", "") in wanted]
            _TICKER_CACHE = tickers
            _TICKER_CACHE_TS = now
            return tickers
        except Exception as e:
            logger.debug("ticker fetch failed: %s", e)
            return _TICKER_CACHE if _TICKER_CACHE else []

    def _collect_proxy_pool(self) -> Dict[str, Any]:
        """从 Proxy Pool Service 获取代理池状态（TTL 缓存, 失败返回降级结构）。"""
        def fetch():
            req = urllib.request.Request(
                f"{PROXY_POOL_API}/status",
                headers={"User-Agent": "DataCollector/1.0"},
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                return json.loads(resp.read().decode("utf-8"))

        try:
            value = self._cached("proxy_pool", fetch)
            if value is not None:
                return value
        except Exception as e:
            logger.debug("Proxy Pool Service 不可用: %s", e)
        return {
            "status": "unavailable",
            "message": "Proxy Pool Service 未运行",
            "total": 0,
            "healthy": 0,
            "unhealthy": 0,
        }

    def _collect_network(self) -> Dict[str, Any]:
        """从 Network Monitor Service 获取网络状态（TTL 缓存, 失败返回降级结构）。"""
        def fetch():
            req = urllib.request.Request(
                f"{NETWORK_MONITOR_API}/status",
                headers={"User-Agent": "DataCollector/1.0"},
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                return json.loads(resp.read().decode("utf-8"))

        try:
            value = self._cached("network", fetch)
            if value is not None:
                return value
        except Exception as e:
            logger.debug("Network Monitor 不可用: %s", e)
        return {
            "status": "unavailable",
            "message": "Network Monitor 未运行",
            "latest": {},
            "stats_1h": {},
            "stats_24h": {},
        }
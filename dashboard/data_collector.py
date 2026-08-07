"""DataCollector — 从各模块聚合数据，供 Dashboard WebSocket 推送。"""

import json
import logging
import urllib.request
from typing import Any, Dict, Optional
from market_data.feed import MarketDataFeed
from portfolio.tracker import PortfolioTracker

logger = logging.getLogger(__name__)

# Proxy Pool Service API 地址
PROXY_POOL_API = "http://127.0.0.1:8765"
# Network Monitor API 地址
NETWORK_MONITOR_API = "http://127.0.0.1:8766"


class DataCollector:
    def __init__(self, feed: MarketDataFeed, portfolio: PortfolioTracker):
        self.feed = feed
        self.portfolio = portfolio

    def collect(self) -> Dict[str, Any]:
        positions = []
        for symbol, pos in self.portfolio.positions.items():
            mark = self.feed.get_mark_price(symbol) or 0.0
            upnl = self.portfolio.unrealized_pnl(symbol, mark)
            positions.append({
                "symbol": symbol,
                "direction": pos.direction,
                "quantity": pos.quantity,
                "entry_price": pos.entry_price,
                "mark_price": round(mark, 2),
                "unrealized_pnl": round(upnl, 2),
            })
        return {
            "equity": round(self.portfolio.total_equity, 2),
            "margin_ratio": round(self.portfolio.margin_ratio, 2),
            "daily_pnl": round(self.portfolio.daily_realized_pnl, 2),
            "drawdown": round(self.portfolio.current_drawdown, 4),
            "position_count": len(positions),
            "positions": positions,
            "prices": self._collect_prices(),
            "proxy_pool": self._collect_proxy_pool(),
            "network": self._collect_network(),
        }

    def _collect_prices(self) -> Dict:
        prices = {}
        for symbol in list(self.portfolio.positions.keys()):
            last = self.feed.get_last_price(symbol)
            mark = self.feed.get_mark_price(symbol)
            if last or mark:
                prices[symbol] = {"last": last, "mark": mark}
        return prices

    def _collect_proxy_pool(self) -> Dict[str, Any]:
        """从 Proxy Pool Service 获取代理池状态。"""
        try:
            req = urllib.request.Request(
                f"{PROXY_POOL_API}/status",
                headers={"User-Agent": "DataCollector/1.0"},
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                return json.loads(resp.read().decode("utf-8"))
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
        """从 Network Monitor Service 获取网络状态。"""
        try:
            req = urllib.request.Request(
                f"{NETWORK_MONITOR_API}/status",
                headers={"User-Agent": "DataCollector/1.0"},
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            logger.debug("Network Monitor 不可用: %s", e)
            return {
                "status": "unavailable",
                "message": "Network Monitor 未运行",
                "latest": {},
                "stats_1h": {},
                "stats_24h": {},
            }
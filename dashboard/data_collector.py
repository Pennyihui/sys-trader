"""DataCollector — 从各模块聚合数据，供 Dashboard WebSocket 推送。"""

import logging
from typing import Any, Dict
from market_data.feed import MarketDataFeed
from portfolio.tracker import PortfolioTracker

logger = logging.getLogger(__name__)


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
        }

    def _collect_prices(self) -> Dict:
        prices = {}
        for symbol in list(self.portfolio.positions.keys()):
            last = self.feed.get_last_price(symbol)
            mark = self.feed.get_mark_price(symbol)
            if last or mark:
                prices[symbol] = {"last": last, "mark": mark}
        return prices

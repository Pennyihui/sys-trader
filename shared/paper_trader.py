"""Paper Trading — 模拟成交，使用实时价格但不发送真实订单。"""

import logging
import random
import time
from dataclasses import dataclass
from typing import Optional

from execution.order_gateway import OrderRequest
from market_data.feed import MarketDataFeed
from shared.database import TradeDatabase

logger = logging.getLogger(__name__)


@dataclass
class PaperFill:
    order_id: int
    symbol: str
    side: str
    quantity: float
    price: float
    status: str = "FILLED"
    executed_qty: float = 0.0
    avg_price: float = 0.0


class PaperTrader:
    """模拟成交引擎。使用 MarketDataFeed 的实时价格模拟成交。

    特征:
    - MARKET 单：基于当前价格 + 滑点立即成交
    - LIMIT 单：价格有利时成交，否则等待（简化版直接成交）
    - 滑点模拟：随机百分比滑点
    """

    def __init__(
        self,
        feed: MarketDataFeed,
        fill_delay_ms: float = 50.0,
        slippage_pct: float = 0.01,
        db: Optional[TradeDatabase] = None,
    ):
        self.feed = feed
        self.fill_delay_ms = fill_delay_ms
        self.slippage_pct = slippage_pct
        self.db = db  # 可选：成交记录持久化，None 时跳过
        self._next_id = 1_000_000
        self._fills: list[PaperFill] = []

    def execute(self, req: OrderRequest) -> PaperFill:
        self._next_id += 1
        current_price = self.feed.get_last_price(req.symbol) or 0.0

        # 模拟延迟
        if self.fill_delay_ms > 0:
            time.sleep(self.fill_delay_ms / 1000.0)

        # 确定成交价
        if req.order_type == "MARKET":
            fill_price = self._apply_slippage(current_price)
        elif req.order_type == "LIMIT" and req.price:
            if (req.side == "BUY" and current_price <= req.price) or (
                req.side == "SELL" and current_price >= req.price
            ):
                fill_price = req.price
            else:
                fill_price = self._apply_slippage(current_price)
        else:
            fill_price = self._apply_slippage(current_price)

        fill = PaperFill(
            order_id=self._next_id,
            symbol=req.symbol,
            side=req.side,
            quantity=req.quantity,
            price=round(fill_price, 2),
            executed_qty=req.quantity,
            avg_price=round(fill_price, 2),
        )
        self._fills.append(fill)
        logger.info(
            "[Paper] %s %s %s @ %.2f", req.side, req.quantity, req.symbol, fill.price
        )

        # 持久化成交记录（可选）
        if self.db is not None:
            try:
                order_id = self.db.create_order(
                    req.symbol, req.side, req.order_type, req.quantity, req.price or 0.0
                )
                self.db.update_order_status(
                    order_id, "FILLED", str(fill.order_id),
                    fill.executed_qty, fill.avg_price,
                )
            except Exception as e:
                logger.warning("Failed to persist paper fill: %s", e)

        return fill

    def _apply_slippage(self, price: float) -> float:
        if price <= 0:
            return 0.0
        slippage = price * random.uniform(-self.slippage_pct, self.slippage_pct)
        return price + slippage

    @property
    def recent_fills(self) -> list[PaperFill]:
        return self._fills[-20:] if self._fills else []

"""Paper Trading — 模拟成交，使用实时价格但不发送真实订单。"""

import logging
import random
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

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
        self._fills: List[PaperFill] = []
        # 挂起的模拟条件单 (SL/TP): order_id -> 订单参数
        self._pending_conditionals: Dict[int, dict] = {}
        # 已触发的条件单成交: order_id -> PaperFill
        self._conditional_fills: Dict[int, PaperFill] = {}
        self._lock = threading.Lock()  # 多线程访问保护 (2026-08-16 审计)

    def execute(self, req: OrderRequest) -> PaperFill:
        with self._lock:
            self._next_id += 1
            order_id = self._next_id
        current_price = self.feed.get_last_price(req.symbol)

        # 模拟延迟
        if self.fill_delay_ms > 0:
            time.sleep(self.fill_delay_ms / 1000.0)

        # 条件单 (STOP_MARKET/TAKE_PROFIT_MARKET 等) 不随市价立即成交:
        # 挂起等待 poll_conditionals() 按行情触发 (与 Algo Order 条件单语义一致)
        if req.order_type not in ("MARKET", "LIMIT"):
            with self._lock:
                self._pending_conditionals[order_id] = {
                    "order_id": order_id,
                    "symbol": req.symbol,
                    "side": req.side,
                    "order_type": req.order_type,
                    "quantity": req.quantity,
                    "trigger_price": req.stop_price,
                    "reduce_only": req.reduce_only,
                }
            logger.info(
                "[Paper] %s %s %s 条件单挂起等待触发 (trigger=%s)",
                req.side, req.quantity, req.symbol,
                req.stop_price if req.stop_price is not None else "?",
            )
            return PaperFill(
                order_id=order_id,
                symbol=req.symbol,
                side=req.side,
                quantity=req.quantity,
                price=0.0,
                status="NEW",
                executed_qty=0.0,
                avg_price=0.0,
            )

        # 无行情时不成交: 以 0 价成交会污染成交记录, 返回 NEW 挂起
        if current_price is None or current_price <= 0:
            logger.warning(
                "[Paper] %s %s %s 无行情, 下单挂起未成交",
                req.side, req.quantity, req.symbol,
            )
            return PaperFill(
                order_id=order_id,
                symbol=req.symbol,
                side=req.side,
                quantity=req.quantity,
                price=0.0,
                status="NEW",
                executed_qty=0.0,
                avg_price=0.0,
            )

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
            order_id=order_id,
            symbol=req.symbol,
            side=req.side,
            quantity=req.quantity,
            price=round(fill_price, 2),
            executed_qty=req.quantity,
            avg_price=round(fill_price, 2),
        )
        with self._lock:
            self._fills.append(fill)
            # 2026-08-16 审计: _fills 只增不删会无界增长, 裁剪到最近 2000 条
            if len(self._fills) > 2000:
                self._fills = self._fills[-2000:]
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

    # ─── 条件单触发轮询 (2026-08-16 审计补缺) ───

    def poll_conditionals(self):
        """按当前行情评估挂起的模拟条件单, 触发即成交。

        触发语义与 Binance 条件单一致:
          - STOP_MARKET:     BUY 价 >= 触发价 / SELL 价 <= 触发价
          - TAKE_PROFIT_MARKET: BUY 价 <= 触发价 / SELL 价 >= 触发价
        优先用标记价 (markPrice), 无标记价回退最近成交价。
        """
        if not self._pending_conditionals:
            return
        triggered: List[int] = []
        # 2026-08-16 审计: 锁内快照, 防多线程写 _pending_conditionals 竞态
        with self._lock:
            pending = list(self._pending_conditionals.items())
        for order_id, c in pending:
            price = self.feed.get_mark_price(c["symbol"])
            if price is None:
                price = self.feed.get_last_price(c["symbol"])
            if price is None or price <= 0:
                continue  # 无行情不成交
            trigger = c["trigger_price"]
            if trigger is None:
                continue
            if c["order_type"] == "STOP_MARKET":
                hit = ((c["side"] == "BUY" and price >= trigger)
                       or (c["side"] == "SELL" and price <= trigger))
            elif c["order_type"] == "TAKE_PROFIT_MARKET":
                hit = ((c["side"] == "BUY" and price <= trigger)
                       or (c["side"] == "SELL" and price >= trigger))
            else:
                hit = False
            if not hit:
                continue
            fill = PaperFill(
                order_id=order_id,
                symbol=c["symbol"],
                side=c["side"],
                quantity=c["quantity"],
                price=round(trigger, 2),
                status="FILLED",
                executed_qty=c["quantity"],
                avg_price=round(trigger, 2),
            )
            with self._lock:
                self._fills.append(fill)
                self._conditional_fills[order_id] = fill
            triggered.append(order_id)
            logger.info(
                "[Paper] 条件单触发 %s %s %s trigger=%.2f price=%.2f",
                c["symbol"], c["order_type"], c["side"], trigger, price,
            )
            if self.db is not None:
                try:
                    db_id = self.db.create_order(
                        c["symbol"], c["side"], c["order_type"],
                        c["quantity"], trigger,
                    )
                    self.db.update_order_status(
                        db_id, "FILLED", str(order_id),
                        fill.executed_qty, fill.avg_price,
                    )
                except Exception as e:
                    logger.warning("Failed to persist conditional fill: %s", e)
        for order_id in triggered:
            with self._lock:
                self._pending_conditionals.pop(order_id, None)

    @property
    def filled_conditional_ids(self) -> set:
        """本轮/历史已触发的条件单 order_id 集合。"""
        with self._lock:
            return set(self._conditional_fills.keys())

    def conditional_fill(self, order_id: int) -> Optional[PaperFill]:
        with self._lock:
            return self._conditional_fills.get(order_id)

    @property
    def pending_conditionals(self) -> Dict[int, dict]:
        with self._lock:
            return dict(self._pending_conditionals)

    @property
    def recent_fills(self) -> list[PaperFill]:
        with self._lock:
            return self._fills[-20:] if self._fills else []

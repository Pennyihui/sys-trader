"""OrderManager -- order lifecycle: submit, retry, timeout, partial fill."""

import math
import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from execution.order_gateway import OrderGateway, OrderRequest, OrderResponse, AlgoOrderRequest, AlgoOrderResponse
from shared.database import TradeDatabase
from shared.execution_mode import ExecutionMode, ExecutionModeManager
from shared.paper_trader import PaperTrader

logger = logging.getLogger(__name__)


def round_price(price: float, tick_size: float = 0.10) -> float:
    """将价格对齐到交易所的 tick size (BTCUSDT: 0.10)"""
    return round(math.floor(price / tick_size) * tick_size, 2)


class OrderState(str, Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    ERROR = "ERROR"


@dataclass
class ManagedOrder:
    order_id: int
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: float
    state: OrderState = OrderState.PENDING
    filled_qty: float = 0.0
    avg_price: float = 0.0
    created_at: float = field(default_factory=time.time)
    error: str = ""
    db_order_id: int = 0  # TradeDatabase orders 表主键


class OrderManager:
    def __init__(
        self,
        gateway: OrderGateway,
        max_retries: int = 3,
        retry_backoff_base: float = 1.0,
        order_timeout: float = 60.0,
        partial_fill_wait: float = 30.0,
        execution_mode: Optional[ExecutionModeManager] = None,
        db: Optional[TradeDatabase] = None,
        paper_trader: Optional[PaperTrader] = None,
        event_bus=None,
        instance: Optional[str] = None,
    ):
        self.gateway = gateway
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base
        self.order_timeout = order_timeout
        self.partial_fill_wait = partial_fill_wait
        # 默认 DRY_RUN：未显式指定模式时不产生真实订单
        self.execution_mode = execution_mode or ExecutionModeManager()
        self.db = db  # 可选：订单生命周期持久化，None 时跳过
        self.paper_trader = paper_trader  # PAPER 模式所需，None 且处于 PAPER 模式时抛错
        self.event_bus = event_bus  # 事件总线（可选，None 时静默）
        # 实例标识（live/paper），默认跟随执行模式（显式传入时优先）
        self.instance = instance or self.execution_mode.mode.value
        self._orders: List[ManagedOrder] = []

    # ─── 执行模式路由 ───

    def _place_paper(self, req: OrderRequest) -> OrderResponse:
        """PAPER 模式：通过 PaperTrader 模拟成交。"""
        if self.paper_trader is None:
            raise RuntimeError("PAPER 模式需要传入 paper_trader 实例")
        fill = self.paper_trader.execute(req)
        return OrderResponse(
            order_id=fill.order_id,
            symbol=fill.symbol,
            side=fill.side,
            status=fill.status,
            executed_qty=fill.executed_qty,
            avg_price=fill.avg_price,
        )

    # ─── 订单生命周期持久化 ───

    def _persist_submit(self, req) -> int:
        """记录订单创建（CREATED）到 TradeDatabase，返回 db order id。"""
        if self.db is None:
            return 0
        price = getattr(req, "price", None) or getattr(req, "trigger_price", None) or 0.0
        return self.db.create_order(req.symbol, req.side, req.order_type, req.quantity, price)

    def _persist_result(self, db_order_id: int, status: str, exchange_order_id: str,
                        filled_qty: float = 0.0, avg_price: float = 0.0, error: str = ""):
        """记录订单状态变更（submitted/filled/rejected）到 TradeDatabase。"""
        if self.db is None or not db_order_id:
            return
        self.db.update_order_status(
            db_order_id, status or "ERROR", exchange_order_id,
            filled_qty, avg_price, 0.0, error,
        )

    def _publish_order(self, resp, req, req_side: str, symbol: str, order_type: str):
        """发布 order.filled 事件到 EventBus（仅成交状态；event_bus 为 None 时静默）。"""
        if self.event_bus is None:
            return
        if resp.status not in ("FILLED", "PARTIALLY_FILLED"):
            return
        # resp 成交字段缺失/为 0 时回退到请求侧（algo 单无 executed_qty/avg_price）
        quantity = getattr(resp, "executed_qty", None)
        if quantity is None:
            quantity = req.quantity
        price = getattr(resp, "avg_price", None)
        if price is None:
            price = getattr(req, "trigger_price", None)
        self.event_bus.publish("order.filled", {
            "instance": self.instance, "symbol": symbol, "side": req_side,
            "order_type": order_type, "status": resp.status,
            "quantity": quantity,
            "price": price,
            "order_id": getattr(resp, "order_id", 0) or getattr(resp, "algo_id", 0),
            "error": getattr(resp, "error", None),
        })

    def _place_with_retry(self, req: OrderRequest) -> OrderResponse:
        mode = self.execution_mode.mode
        if mode == ExecutionMode.DRY_RUN:
            # 只读模式：模拟 NEW 状态，不调用交易所
            return OrderResponse(
                order_id=0,
                symbol=req.symbol,
                side=req.side,
                status="NEW",
                executed_qty=0.0,
                avg_price=0.0,
            )
        if mode == ExecutionMode.PAPER:
            return self._place_paper(req)
        # LIVE：当前行为，走真实 gateway
        last_error = None
        for attempt in range(self.max_retries):
            resp = self.gateway.place_order(req)
            if resp.status != "ERROR":
                return resp
            last_error = resp.error
            time.sleep(self.retry_backoff_base * (2**attempt))
        return OrderResponse(
            order_id=0,
            symbol=req.symbol,
            side=req.side,
            status="ERROR",
            executed_qty=0.0,
            avg_price=0.0,
            error=last_error or "Max retries exceeded",
        )

    def _place_algo_with_retry(self, req: AlgoOrderRequest) -> AlgoOrderResponse:
        """Algo Order API 重试封装，与 _place_with_retry 同一模式。"""
        mode = self.execution_mode.mode
        if mode == ExecutionMode.DRY_RUN:
            # 只读模式：模拟 NEW 状态，不调用交易所
            return AlgoOrderResponse(
                algo_id=0, symbol=req.symbol, side=req.side, status="NEW"
            )
        if mode == ExecutionMode.PAPER:
            if self.paper_trader is None:
                raise RuntimeError("PAPER 模式需要传入 paper_trader 实例")
            fill = self.paper_trader.execute(
                OrderRequest(
                    symbol=req.symbol, side=req.side, order_type=req.order_type,
                    quantity=req.quantity, stop_price=req.trigger_price,
                    reduce_only=req.reduce_only,
                )
            )
            return AlgoOrderResponse(
                algo_id=fill.order_id, symbol=req.symbol,
                side=req.side, status=fill.status,
            )
        # LIVE：当前行为，走真实 gateway
        last_error = None
        for attempt in range(self.max_retries):
            resp = self.gateway.place_algo_order(req)
            if resp.status != "ERROR":
                return resp
            last_error = resp.error
            time.sleep(self.retry_backoff_base * (2**attempt))
        return AlgoOrderResponse(
            algo_id=0, symbol=req.symbol,
            side=req.side, status="ERROR",
            error=last_error or "Max retries exceeded",
        )

    def submit_entry(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
    ) -> ManagedOrder:
        side = "BUY" if direction == "LONG" else "SELL"
        req = OrderRequest(
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            quantity=quantity,
            price=entry_price,
        )
        db_order_id = self._persist_submit(req)
        resp = self._place_with_retry(req)
        state = (
            OrderState.REJECTED
            if resp.status in ("REJECTED", "ERROR")
            else OrderState.PENDING
        )
        order = ManagedOrder(
            order_id=resp.order_id,
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            quantity=quantity,
            price=entry_price,
            state=state,
            error=resp.error or "",
            db_order_id=db_order_id,
        )
        self._orders.append(order)
        self._persist_result(
            db_order_id, resp.status,
            str(resp.order_id) if resp.order_id else "",
            resp.executed_qty, resp.avg_price, resp.error or "",
        )
        self._publish_order(resp, req, side, symbol, req.order_type)
        return order

    def submit_stop_loss(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        stop_price: float,
    ) -> ManagedOrder:
        """通过 Algo Order API 下达止损条件单。"""
        side = "SELL" if direction == "LONG" else "BUY"
        req = AlgoOrderRequest(
            symbol=symbol, side=side,
            order_type="STOP_MARKET",
            quantity=quantity,
            trigger_price=round_price(stop_price),
            reduce_only=True,
        )
        db_order_id = self._persist_submit(req)
        resp = self._place_algo_with_retry(req)
        state = OrderState.REJECTED if resp.status in ("REJECTED", "ERROR") else OrderState.PENDING
        order = ManagedOrder(
            order_id=resp.algo_id or 0,
            symbol=symbol, side=side, order_type="STOP_MARKET",
            quantity=quantity, price=stop_price,
            state=state, error=resp.error or "",
            db_order_id=db_order_id,
        )
        self._orders.append(order)
        self._persist_result(
            db_order_id, resp.status,
            str(resp.algo_id) if resp.algo_id else "",
            error=resp.error or "",
        )
        self._publish_order(resp, req, side, symbol, req.order_type)
        return order

    def submit_take_profit(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        tp_price: float,
    ) -> ManagedOrder:
        """通过 Algo Order API 下达止盈条件单。"""
        side = "SELL" if direction == "LONG" else "BUY"
        req = AlgoOrderRequest(
            symbol=symbol, side=side,
            order_type="TAKE_PROFIT_MARKET",
            quantity=quantity,
            trigger_price=round_price(tp_price),
            reduce_only=True,
        )
        db_order_id = self._persist_submit(req)
        resp = self._place_algo_with_retry(req)
        state = OrderState.REJECTED if resp.status in ("REJECTED", "ERROR") else OrderState.PENDING
        order = ManagedOrder(
            order_id=resp.algo_id or 0,
            symbol=symbol, side=side, order_type="TAKE_PROFIT_MARKET",
            quantity=quantity, price=tp_price,
            state=state, error=resp.error or "",
            db_order_id=db_order_id,
        )
        self._orders.append(order)
        self._persist_result(
            db_order_id, resp.status,
            str(resp.algo_id) if resp.algo_id else "",
            error=resp.error or "",
        )
        self._publish_order(resp, req, side, symbol, req.order_type)
        return order

    def execute_signal(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
    ) -> List[ManagedOrder]:
        orders = []
        orders.append(
            self.submit_entry(symbol, direction, quantity, entry_price, stop_loss, take_profit)
        )
        orders.append(self.submit_stop_loss(symbol, direction, quantity, stop_loss))
        orders.append(self.submit_take_profit(symbol, direction, quantity, take_profit))
        return orders

    @property
    def active_orders(self) -> List[ManagedOrder]:
        return [
            o
            for o in self._orders
            if o.state in (OrderState.PENDING, OrderState.PARTIALLY_FILLED)
        ]

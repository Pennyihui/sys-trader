"""OrderManager -- order lifecycle: submit, retry, timeout, partial fill."""

import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List
from execution.order_gateway import OrderGateway, OrderRequest, OrderResponse

logger = logging.getLogger(__name__)


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


class OrderManager:
    def __init__(
        self,
        gateway: OrderGateway,
        max_retries: int = 3,
        retry_backoff_base: float = 1.0,
        order_timeout: float = 60.0,
        partial_fill_wait: float = 30.0,
    ):
        self.gateway = gateway
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base
        self.order_timeout = order_timeout
        self.partial_fill_wait = partial_fill_wait
        self._orders: List[ManagedOrder] = []

    def _place_with_retry(self, req: OrderRequest) -> OrderResponse:
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
        )
        self._orders.append(order)
        return order

    def submit_stop_loss(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        stop_price: float,
    ) -> ManagedOrder:
        side = "SELL" if direction == "LONG" else "BUY"
        req = OrderRequest(
            symbol=symbol,
            side=side,
            order_type="STOP_MARKET",
            quantity=quantity,
            stop_price=stop_price,
            reduce_only=True,
        )
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
            order_type="STOP_MARKET",
            quantity=quantity,
            price=stop_price,
            state=state,
            error=resp.error or "",
        )
        self._orders.append(order)
        return order

    def submit_take_profit(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        tp_price: float,
    ) -> ManagedOrder:
        side = "SELL" if direction == "LONG" else "BUY"
        req = OrderRequest(
            symbol=symbol,
            side=side,
            order_type="TAKE_PROFIT_MARKET",
            quantity=quantity,
            stop_price=tp_price,
            reduce_only=True,
        )
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
            order_type="TAKE_PROFIT_MARKET",
            quantity=quantity,
            price=tp_price,
            state=state,
            error=resp.error or "",
        )
        self._orders.append(order)
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

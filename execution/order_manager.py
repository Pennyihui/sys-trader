"""OrderManager -- order lifecycle: submit, retry, timeout, partial fill."""

import math
import os
import threading
import time
import uuid
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
    """将价格向下对齐到交易所的 tick size（8 位小数防浮点尾差）。

    旧实现 round(..., 2) 固定 2 位小数, 对 SOL 等 tick=0.001 的标的会
    破坏档位精度 (2026-08-16 审计修复)。向下取整保证 SL 更保守、TP 更易触发。
    """
    return round(math.floor(price / tick_size) * tick_size, 8)


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
    # 入场单携带的保护价 (成交后补挂 SL/TP 用, 2026-08-16 审计)
    stop_price: float = 0.0
    take_profit: float = 0.0
    # 幂等键: 重试复用, 交易所按 newClientOrderId 去重
    client_order_id: str = ""
    # 部分成交余量策略 (2026-08-16 #9): policy=cancel 时标记已请求撤余量
    remainder_canceled: bool = False


class OrderManager:
    # 内置默认 tickSize（exchangeInfo 拉取失败时的退化档位）;
    # 未知 symbol 退化为 0.01（ETH 档）。
    # 注: SOLUSDT 实际 tickSize=0.01 (2026-08-16 实测 exchangeInfo)。
    _DEFAULT_TICKS = {"BTCUSDT": 0.10, "ETHUSDT": 0.01, "SOLUSDT": 0.01}

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
        tick_sizes: Optional[dict] = None,
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
        # 价格精度: symbol -> tickSize (runner 从 exchangeInfo 拉取后回填)
        self.tick_sizes = tick_sizes or {}
        # postOnly (P1-3): LIMIT_MAKER 挂单只吃 maker 费率 (0.05%→0.02%)
        self.post_only = os.environ.get("POST_ONLY", "0") == "1"
        # 入场单有效期 (2026-08-16 #8): GTC 挂单等待成交 / IOC 立即成交否则撤
        self.entry_tif = os.environ.get("ENTRY_TIF", "GTC").upper()
        if self.entry_tif not in ("GTC", "IOC"):
            logger.warning("ENTRY_TIF=%s 非法, 退化为 GTC", self.entry_tif)
            self.entry_tif = "GTC"
        # 部分成交余量策略 (2026-08-16 #9): wait=保留余量继续等 / cancel=撤余量按已成交建仓
        self.partial_fill_policy = os.environ.get("PARTIAL_FILL_POLICY", "wait")
        if self.partial_fill_policy not in ("wait", "cancel"):
            logger.warning("PARTIAL_FILL_POLICY=%s 非法, 退化为 wait", self.partial_fill_policy)
            self.partial_fill_policy = "wait"
        # 保护单触发基准 (2026-08-16 #4): CONTRACT_PRICE=最新价(默认) / MARK_PRICE=标记价
        self.working_type = os.environ.get("PROTECTION_WORKING_TYPE", "CONTRACT_PRICE")
        if self.working_type not in ("CONTRACT_PRICE", "MARK_PRICE"):
            logger.warning("PROTECTION_WORKING_TYPE=%s 非法, 退化为 CONTRACT_PRICE", self.working_type)
            self.working_type = "CONTRACT_PRICE"
        # 止损保护模式 (2026-08-16 #5): stop=固定止损 / trailing=追踪止损
        self.sl_mode = os.environ.get("PROTECTION_SL_MODE", "stop")
        if self.sl_mode not in ("stop", "trailing"):
            logger.warning("PROTECTION_SL_MODE=%s 非法, 退化为 stop", self.sl_mode)
            self.sl_mode = "stop"
        try:
            self.trailing_callback = float(os.environ.get("TRAILING_STOP_CALLBACK", "1"))
        except (TypeError, ValueError):
            self.trailing_callback = 1.0
        if self.sl_mode == "trailing" and not (0.1 <= self.trailing_callback <= 5):
            logger.warning("TRAILING_STOP_CALLBACK=%.2f 超出 [0.1, 5], 退化为 1", self.trailing_callback)
            self.trailing_callback = 1.0
        self._orders: List[ManagedOrder] = []
        # 订单列表锁: 主循环 / user stream 推送线程 / 轮询线程并发读写
        # (2026-08-16 审计: _orders 此前四线程无锁, 存在迭代竞态)
        self._lock = threading.Lock()

    def _append_order(self, order: ManagedOrder):
        with self._lock:
            self._orders.append(order)
            self._prune_terminal()

    def _orders_snapshot(self) -> List[ManagedOrder]:
        with self._lock:
            return list(self._orders)

    def _tick(self, symbol: str) -> float:
        return self.tick_sizes.get(symbol) or self._DEFAULT_TICKS.get(symbol, 0.01)

    def _align_price(self, symbol: str, price: float) -> float:
        return round_price(price, self._tick(symbol))

    def _prune_terminal(self):
        """归档已终态订单: 列表超上限时丢弃最老的终态单, 防长跑内存增长。"""
        if len(self._orders) <= 1000:
            return
        keep = [
            o for o in self._orders
            if o.state in (OrderState.PENDING, OrderState.PARTIALLY_FILLED)
        ]
        terminal = [
            o for o in self._orders
            if o.state not in (OrderState.PENDING, OrderState.PARTIALLY_FILLED)
        ]
        # 保留全部活跃单 + 最近 500 条终态单
        self._orders = keep + terminal[-500:]

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
        # 入场价对齐 tickSize, 避免 PRICE_FILTER 拒单 (BTC tick=0.10 等)
        aligned_price = self._align_price(symbol, entry_price)
        client_order_id = f"e{uuid.uuid4().hex[:28]}"  # 幂等键, 重试复用
        # IOC 与 LIMIT_MAKER 互斥 (2026-08-16 #8): IOC 需要 timeInForce,
        # postOnly 是 maker 挂单语义, 两者不能同时存在
        post_only = self.post_only and self.entry_tif == "GTC"
        req = OrderRequest(
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            quantity=quantity,
            price=aligned_price,
            time_in_force=self.entry_tif,
            client_order_id=client_order_id,
            post_only=post_only,
        )
        db_order_id = self._persist_submit(req)
        resp = self._place_with_retry(req)
        # 幂等恢复: 响应丢失/超时后服务器可能已接受该 clientOrderId 的订单,
        # 返回 -2010/-2011 (重复 id) 时按 id 查回真实订单状态, 而非判失败重下
        if resp.status == "REJECTED" and client_order_id and resp.code in (-2010, -2011):
            resp = self._recover_by_client_id(req, resp)
        # 状态映射: 已成交 (FILLED/PARTIALLY_FILLED) 与待成交 (NEW) 区分,
        # 仅 REJECTED/ERROR 记为失败; NEW → PENDING 保留在活跃集等待成交
        if resp.status in ("REJECTED", "ERROR"):
            state = OrderState.REJECTED
        elif resp.status == "FILLED":
            state = OrderState.FILLED
        elif resp.status == "PARTIALLY_FILLED":
            state = OrderState.PARTIALLY_FILLED
        else:
            state = OrderState.PENDING
        order = ManagedOrder(
            order_id=resp.order_id,
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            quantity=quantity,
            price=aligned_price,
            state=state,
            filled_qty=resp.executed_qty or 0.0,
            avg_price=resp.avg_price or 0.0,
            error=resp.error or "",
            db_order_id=db_order_id,
            stop_price=self._align_price(symbol, stop_loss),
            take_profit=self._align_price(symbol, take_profit),
            client_order_id=client_order_id,
        )
        self._append_order(order)
        self._persist_result(
            db_order_id, resp.status,
            str(resp.order_id) if resp.order_id else "",
            resp.executed_qty, resp.avg_price, resp.error or "",
        )
        self._publish_order(resp, req, side, symbol, req.order_type)
        # 部分成交余量策略 (2026-08-16 #9): 下单即部分成交时立即处理余量
        if order.state == OrderState.PARTIALLY_FILLED:
            self._maybe_cancel_remainder(order)
        return order

    def _recover_by_client_id(self, req: OrderRequest, resp: OrderResponse) -> OrderResponse:
        """按 clientOrderId 查回订单真实状态 (幂等恢复路径)。"""
        data = self.gateway.query_order_by_client_id(req.symbol, req.client_order_id)
        if not data:
            return resp
        status = data.get("status", "NEW")
        logger.warning(
            "Idempotency recovery: clientOrderId=%s → 已存在订单 %s (status=%s)",
            req.client_order_id, data.get("orderId"), status,
        )
        return OrderResponse(
            order_id=int(data.get("orderId", 0)),
            symbol=req.symbol,
            side=req.side,
            status=status,
            executed_qty=float(data.get("executedQty", 0)),
            avg_price=float(data.get("avgPrice", 0)),
            error=None,
        )

    def submit_stop_loss(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        stop_price: float,
    ) -> ManagedOrder:
        """通过 Algo Order API 下达止损条件单。

        PROTECTION_SL_MODE=trailing 时改为追踪止损 (2026-08-16 #5):
        盈利后保护价自动上移, 回调 callbackRate% 触发, 止盈不停奔跑。
        """
        if self.sl_mode == "trailing" and self.execution_mode.mode == ExecutionMode.LIVE:
            return self.submit_trailing_stop(symbol, direction, quantity)
        # PAPER/DRY_RUN: 无真实追踪止损支持, 退化为固定止损 (防递归)
        side = "SELL" if direction == "LONG" else "BUY"
        aligned_stop = self._align_price(symbol, stop_price)
        req = AlgoOrderRequest(
            symbol=symbol, side=side,
            order_type="STOP_MARKET",
            quantity=quantity,
            trigger_price=aligned_stop,
            reduce_only=True,
            working_type=self.working_type,
        )
        db_order_id = self._persist_submit(req)
        resp = self._place_algo_with_retry(req)
        state = OrderState.REJECTED if resp.status in ("REJECTED", "ERROR") else OrderState.PENDING
        order = ManagedOrder(
            order_id=resp.algo_id or 0,
            symbol=symbol, side=side, order_type="STOP_MARKET",
            quantity=quantity, price=aligned_stop,
            state=state, error=resp.error or "",
            db_order_id=db_order_id,
        )
        self._append_order(order)
        self._persist_result(
            db_order_id, resp.status,
            str(resp.algo_id) if resp.algo_id else "",
            error=resp.error or "",
        )
        self._publish_order(resp, req, side, symbol, req.order_type)
        return order

    def submit_trailing_stop(
        self,
        symbol: str,
        direction: str,
        quantity: float,
    ) -> ManagedOrder:
        """追踪止损 (TRAILING_STOP_MARKET, 2026-08-16 #5)。

        callbackRate = TRAILING_STOP_CALLBACK (默认 1%); 触发基准跟随 workingType。
        失败时返回 REJECTED (保护缺失由 runner 日志+告警, fail-correct)。
        """
        side = "SELL" if direction == "LONG" else "BUY"
        req = AlgoOrderRequest(
            symbol=symbol, side=side,
            order_type="TRAILING_STOP_MARKET",
            quantity=quantity,
            callback_rate=self.trailing_callback,
            reduce_only=True,
            working_type=self.working_type,
        )
        db_order_id = self._persist_submit(req)
        resp = self._place_algo_with_retry(req)
        state = OrderState.REJECTED if resp.status in ("REJECTED", "ERROR") else OrderState.PENDING
        order = ManagedOrder(
            order_id=resp.algo_id or 0,
            symbol=symbol, side=side, order_type="TRAILING_STOP_MARKET",
            quantity=quantity, price=0.0,
            state=state, error=resp.error or "",
            db_order_id=db_order_id,
        )
        self._append_order(order)
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
        aligned_tp = self._align_price(symbol, tp_price)
        req = AlgoOrderRequest(
            symbol=symbol, side=side,
            order_type="TAKE_PROFIT_MARKET",
            quantity=quantity,
            trigger_price=aligned_tp,
            reduce_only=True,
            working_type=self.working_type,
        )
        db_order_id = self._persist_submit(req)
        resp = self._place_algo_with_retry(req)
        state = OrderState.REJECTED if resp.status in ("REJECTED", "ERROR") else OrderState.PENDING
        order = ManagedOrder(
            order_id=resp.algo_id or 0,
            symbol=symbol, side=side, order_type="TAKE_PROFIT_MARKET",
            quantity=quantity, price=aligned_tp,
            state=state, error=resp.error or "",
            db_order_id=db_order_id,
        )
        self._append_order(order)
        self._persist_result(
            db_order_id, resp.status,
            str(resp.algo_id) if resp.algo_id else "",
            error=resp.error or "",
        )
        self._publish_order(resp, req, side, symbol, req.order_type)
        return order

    @staticmethod
    def validate_protection(direction: str, entry_price: float,
                            stop_loss: float, take_profit: float) -> Optional[str]:
        """校验 SL/TP 与方向的价格几何关系, 违规返回原因, 合规返回 None。

        LONG: 止损 < 入场 < 止盈; SHORT: 止盈 < 入场 < 止损。
        防止策略 bug 挂出方向颠倒的保护单 (2026-08-16 审计)。
        """
        if entry_price <= 0:
            return "invalid entry price"
        if direction == "LONG":
            if stop_loss >= entry_price:
                return f"LONG stop_loss {stop_loss} >= entry {entry_price}"
            if take_profit <= entry_price:
                return f"LONG take_profit {take_profit} <= entry {entry_price}"
        elif direction == "SHORT":
            if stop_loss <= entry_price:
                return f"SHORT stop_loss {stop_loss} <= entry {entry_price}"
            if take_profit >= entry_price:
                return f"SHORT take_profit {take_profit} >= entry {entry_price}"
        return None

    def execute_signal(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
    ) -> List[ManagedOrder]:
        # 保护价几何校验: 违规直接拒绝整单 (宁可不入场, 不裸仓)
        invalid = self.validate_protection(direction, entry_price, stop_loss, take_profit)
        if invalid:
            logger.error("SIGNAL REJECTED %s: %s", symbol, invalid)
            return [ManagedOrder(
                order_id=0, symbol=symbol,
                side="BUY" if direction == "LONG" else "SELL",
                order_type="LIMIT", quantity=quantity, price=entry_price,
                state=OrderState.REJECTED, error=invalid,
            )]
        # 先下入场单, 若被拒/出错则跳过止损/止盈, 避免对未成交仓位挂保护单
        entry = self.submit_entry(symbol, direction, quantity, entry_price, stop_loss, take_profit)
        if entry.state in (OrderState.REJECTED, OrderState.ERROR):
            return [entry]
        orders = [entry]
        # 2026-08-16 审计: 仅入场已成交 (FILLED/PARTIALLY_FILLED) 时立即挂 SL/TP;
        # PENDING 时延后到 sync_entry_fills 确认成交后再挂, 避免:
        # ① 成交前条件单先触发被拒 (reduce_only 无仓可减) → 成交后裸仓
        # ② 未成交即登记持仓造成的"幽灵持仓"
        if entry.state in (OrderState.FILLED, OrderState.PARTIALLY_FILLED):
            qty = entry.filled_qty or entry.quantity
            orders.append(self.submit_stop_loss(symbol, direction, qty, stop_loss))
            orders.append(self.submit_take_profit(symbol, direction, qty, take_profit))
        else:
            logger.info("Entry %s %s PENDING — SL/TP 延后至成交确认后再挂",
                        symbol, entry.order_id)
        return orders

    def _maybe_cancel_remainder(self, order: ManagedOrder):
        """部分成交余量策略 (2026-08-16 #9): PARTIAL_FILL_POLICY=cancel 时
        撤掉未成交余量, 按已成交数量建仓 (避免余量在不利价位再成交扩大仓位)。

        撤单失败 (ERROR=网络故障) 时保持 PARTIALLY_FILLED 并复位标记,
        下轮重试 — 与 runner._cancel_one_order 同一 fail-correct 口径。
        """
        if self.partial_fill_policy != "cancel":
            return
        if order.state != OrderState.PARTIALLY_FILLED:
            return
        if getattr(order, "remainder_canceled", False):
            return
        if self.execution_mode.mode != ExecutionMode.LIVE:
            return  # PAPER/DRY_RUN 无真实余量挂单
        order.remainder_canceled = True  # 置位防重复请求
        resp = self.gateway.cancel_order(order.symbol, order.order_id)
        status = getattr(resp, "status", "")
        if status in ("CANCELED", "REJECTED"):
            order.state = OrderState.CANCELED
            self._persist_result(order.db_order_id, "CANCELED", str(order.order_id),
                                 order.filled_qty, order.avg_price)
            logger.warning("PARTIAL FILL cancel: %s 已成交 %s, 余量 %s 已撤 — 按已成交建仓",
                           order.symbol, order.filled_qty,
                           round(order.quantity - (order.filled_qty or 0), 8))
        elif status == "ERROR":
            order.remainder_canceled = False  # 网络失败: 下轮重试
            logger.error("PARTIAL FILL 撤余量失败 %s (%s) — 保持等待重试",
                         order.symbol, getattr(resp, "error", ""))
        else:
            logger.warning("PARTIAL FILL 撤余量未确认 %s status=%s: %s",
                           order.symbol, status, getattr(resp, "error", ""))

    def sync_entry_fills(self) -> List[ManagedOrder]:
        """轮询 PENDING 入场单的交易所状态, 返回"本轮新确认成交"的入场单列表。

        2026-08-16 审计补缺: 全系统此前无订单状态轮询, LIMIT 未成交即登记持仓。
        本方法仅 LIVE 模式调用 (PAPER/DRY_RUN 不触真实网关); 查询失败保持原状态。
        """
        if self.execution_mode.mode != ExecutionMode.LIVE:
            return []
        newly_filled: List[ManagedOrder] = []
        for order in self._orders_snapshot():
            if order.order_type != "LIMIT":
                continue
            if order.state not in (OrderState.PENDING, OrderState.PARTIALLY_FILLED):
                continue
            data = self.gateway.query_order_status(order.symbol, order.order_id)
            if not data:
                continue
            status = data.get("status", "")
            executed = float(data.get("executedQty", 0) or 0)
            avg = float(data.get("avgPrice", 0) or 0)
            prev = order.state
            old_filled = order.filled_qty
            if status == "FILLED":
                order.state = OrderState.FILLED
            elif status == "PARTIALLY_FILLED":
                order.state = OrderState.PARTIALLY_FILLED
            elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                order.state = OrderState.CANCELED if status in ("CANCELED", "EXPIRED") else OrderState.REJECTED
                order.error = f"exchange status: {status}"
                self._persist_result(order.db_order_id, status, str(order.order_id), executed, avg)
                continue
            else:
                continue  # NEW 等仍 PENDING
            order.filled_qty = executed
            order.avg_price = avg
            # D2 (2026-08-16): 部分成交余量增量也触发补登记/补保护
            if (prev == OrderState.PENDING
                    or (prev == OrderState.PARTIALLY_FILLED and executed > old_filled)):
                newly_filled.append(order)
                logger.info("ENTRY FILLED %s orderId=%s qty=%s avg=%s",
                            order.symbol, order.order_id, executed, avg)
            self._persist_result(order.db_order_id, status, str(order.order_id), executed, avg)
            # 部分成交余量策略 (2026-08-16 #9)
            if order.state == OrderState.PARTIALLY_FILLED:
                self._maybe_cancel_remainder(order)
        return newly_filled

    def on_user_order_update(self, data: dict) -> List[ManagedOrder]:
        """User Data Stream ORDER_TRADE_UPDATE 推送处理 (2026-08-16 补缺)。

        按 orderId / clientOrderId 匹配本地 PENDING 入场单, 成交即更新状态;
        返回"本轮新确认成交"的入场单列表 (runner 据此登记持仓 + 补挂 SL/TP)。
        匹配不到的订单 (如条件单触发后的真实订单) 静默忽略。
        """
        order_id = int(data.get("i", 0) or 0)
        client_id = data.get("c", "") or ""
        status = data.get("X", "")
        executed = float(data.get("z", 0) or 0)
        avg = float(data.get("ap", 0) or 0)
        newly_filled: List[ManagedOrder] = []
        for order in self._orders_snapshot():
            if order.order_type != "LIMIT":
                continue
            if order.order_id != order_id and (not client_id or order.client_order_id != client_id):
                continue
            prev = order.state
            old_filled = order.filled_qty
            if status in ("FILLED", "PARTIALLY_FILLED"):
                order.state = (OrderState.FILLED if status == "FILLED"
                               else OrderState.PARTIALLY_FILLED)
                order.filled_qty = executed
                order.avg_price = avg
                # D2 (2026-08-16): 部分成交余量增量也触发补登记/补保护
                if (prev == OrderState.PENDING
                        or (prev == OrderState.PARTIALLY_FILLED
                            and executed > old_filled)):
                    newly_filled.append(order)
                    logger.info("USER STREAM ENTRY FILLED %s orderId=%s qty=%s avg=%s",
                                order.symbol, order_id, executed, avg)
                self._persist_result(order.db_order_id, status, str(order_id), executed, avg)
                # 部分成交余量策略 (2026-08-16 #9)
                if order.state == OrderState.PARTIALLY_FILLED:
                    self._maybe_cancel_remainder(order)
            elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                order.state = (OrderState.CANCELED if status in ("CANCELED", "EXPIRED")
                               else OrderState.REJECTED)
                order.error = f"user stream: {status}"
                self._persist_result(order.db_order_id, status, str(order_id), executed, avg)
            break
        return newly_filled

    def sync_algo_orders(self) -> List[str]:
        """轮询开放条件单清单, 判定本地 PENDING 保护单是否已触发 (2026-08-16 S1)。

        触发判定: 本地 algoId 不在交易所"开放条件单"清单 → 已触发/成交。
        返回"保护单已触发"的 symbol 列表 (runner 据此撤残余保护单+同步平仓)。
        查询失败时保持原状态 (None → 跳过该 symbol)。仅 LIVE 模式调用。
        """
        if self.execution_mode.mode != ExecutionMode.LIVE:
            return []
        by_symbol: dict = {}
        for o in self._orders_snapshot():
            if o.order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET") \
                    and o.state == OrderState.PENDING and o.order_id:
                by_symbol.setdefault(o.symbol, []).append(o)
        triggered: List[str] = []
        for symbol, orders in by_symbol.items():
            open_ids = self.gateway.get_open_algo_orders(symbol)
            if open_ids is None:
                continue
            for o in orders:
                if o.order_id in open_ids:
                    continue  # 仍在开放清单 → 未触发
                o.state = OrderState.FILLED
                o.error = "triggered"
                self._persist_result(o.db_order_id, "FILLED", str(o.order_id),
                                     o.quantity, o.price)
                logger.info("ALGO TRIGGERED %s %s algoId=%s",
                            symbol, o.order_type, o.order_id)
                triggered.append(symbol)
        return sorted(set(triggered))

    def place_protection(self, order: ManagedOrder, qty: Optional[float] = None) -> List[ManagedOrder]:
        """入场成交确认后补挂 SL/TP 保护单 (PENDING 入场单成交后的延后挂单)。

        2026-08-16 审计修复 (S3): 用实际成交价 (avg_price) 校验几何关系——
        LIMIT 迟到成交的成交价可能远偏离信号价, LONG 止损高于成交价会挂出
        即秒损; 几何非法时拒绝挂单并告警 (宁裸仓报警, 不挂反向秒损单)。
        qty 参数: 部分成交增量补挂时显式指定数量 (D2)。
        """
        direction = "LONG" if order.side == "BUY" else "SHORT"
        quantity = qty if qty is not None else (order.filled_qty or order.quantity)
        if quantity <= 0 or order.stop_price <= 0 or order.take_profit <= 0:
            logger.error("PROTECTION SKIP %s: 缺保护价或数量 (qty=%s sl=%s tp=%s)",
                         order.symbol, quantity, order.stop_price, order.take_profit)
            return []
        fill_price = order.avg_price or order.price
        invalid = self.validate_protection(
            direction, fill_price, order.stop_price, order.take_profit)
        if invalid:
            logger.error(
                "PROTECTION SKIP %s: 保护价与成交价 %.2f 几何冲突 (%s) — "
                "持仓无保护, 请人工处理!", order.symbol, fill_price, invalid)
            return []
        return [
            self.submit_stop_loss(order.symbol, direction, quantity, order.stop_price),
            self.submit_take_profit(order.symbol, direction, quantity, order.take_profit),
        ]

    def poll_paper_conditionals(self):
        """PAPER 模式: 轮询模拟盘条件单触发（2026-08-16 审计补缺）。

        模拟盘 SL/TP 此前永不成交 (条件单返回 NEW 后无触发机制), 仓位
        保护形同虚设。此处委托 PaperTrader 按行情触发, 并把已触发的
        ManagedOrder 置为 FILLED + 发布 order.filled 事件 (供 dashboard/
        shadow monitor 消费)。非 PAPER 模式为 no-op。
        """
        if (self.execution_mode.mode != ExecutionMode.PAPER
                or self.paper_trader is None):
            return
        self.paper_trader.poll_conditionals()
        filled_ids = self.paper_trader.filled_conditional_ids
        if not filled_ids:
            return
        for o in self._orders_snapshot():
            if o.state != OrderState.PENDING:
                continue
            if o.order_type not in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
                continue
            if o.order_id not in filled_ids:
                continue
            fill = self.paper_trader.conditional_fill(o.order_id)
            o.state = OrderState.FILLED
            o.filled_qty = fill.executed_qty if fill else o.quantity
            o.avg_price = fill.avg_price if fill else o.price
            if self.event_bus is not None:
                self.event_bus.publish("order.filled", {
                    "instance": self.instance, "symbol": o.symbol,
                    "side": o.side, "order_type": o.order_type,
                    "status": "FILLED",
                    "quantity": o.filled_qty,
                    "price": o.avg_price,
                    "order_id": o.order_id,
                    "error": None,
                })
            logger.info(
                "PAPER CONDITIONAL FILLED %s %s %s @ %.2f",
                o.symbol, o.order_type, o.side, o.avg_price,
            )

    @property
    def active_orders(self) -> List[ManagedOrder]:
        return [
            o
            for o in self._orders_snapshot()
            if o.state in (OrderState.PENDING, OrderState.PARTIALLY_FILLED)
        ]

"""PortfolioTracker — position, equity, margin, PnL tracking."""

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


@dataclass
class Position:
    symbol: str
    direction: str
    quantity: float
    entry_price: float
    leverage: int
    opened_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class PortfolioTracker:
    def __init__(self, initial_equity: float = 0.0, event_bus=None, instance: str = "live",
                 fee_rate: float = 0.001):
        self.total_equity: float = initial_equity
        self.available_balance: float = initial_equity
        self.peak_equity: float = initial_equity
        self.daily_realized_pnl: float = 0.0
        self.total_realized_pnl: float = 0.0
        self.positions: Dict[str, Position] = {}
        self.trade_count_today: int = 0
        self.consecutive_losses: int = 0
        self._last_reset_day = datetime.now(timezone.utc).date()
        self.event_bus = event_bus  # 事件总线注入（可选，None 时静默跳过）
        self.instance = instance  # 实例标识（live/paper），随事件发布供消费侧过滤
        # 手续费模型 (2026-08-16 P0-3): 往返 taker 费率合计 (0.05%×2),
        # 已实现盈亏 = 毛盈亏 - 手续费, 使连亏/日亏阈值口径真实
        self.fee_rate = fee_rate
        self.total_fees: float = 0.0
        # 资金费累计 (2026-08-16 补记账): 每 8h 结算周期估算成本计入
        self.total_funding_fees: float = 0.0
        # 多线程竞态保护: scheduler / reconciler / 主循环并发读写 (2026-08-16 审计)
        self._lock = threading.RLock()

    def _publish(self, data: dict):
        """发布 position.changed 事件；未注入 event_bus 时静默跳过。"""
        if self.event_bus is not None:
            self.event_bus.publish("position.changed", {**data, "instance": self.instance})

    def _maybe_reset_daily(self):
        """日切重置 (调用方需持锁)。用 date 比较而非 .day——旧实现跨月同日会漏重置。"""
        today = datetime.now(timezone.utc).date()
        if today != self._last_reset_day:
            self.daily_realized_pnl = 0.0
            self.trade_count_today = 0
            self._last_reset_day = today

    def update_equity(self, total_equity: float, available_balance: Optional[float] = None,
                      assets: Optional[list] = None):
        with self._lock:
            self.total_equity = total_equity
            if available_balance is not None:
                self.available_balance = available_balance
            if total_equity > self.peak_equity:
                self.peak_equity = total_equity
            payload = {"event": "equity", "total_equity": total_equity, "available_balance": self.available_balance,
                       "margin_ratio": self.margin_ratio, "daily_pnl": self.daily_realized_pnl,
                       "drawdown": self.current_drawdown,
                       # 实际费率随事件下发 (2026-08-16 #1: 面板保本价/盈亏口径同源)
                       "fee_rate": self.fee_rate,
                       # 资产构成明细 (USDT/USDC/BTC...), 2026-08-16: 防"权益缩水"误读
                       "assets": assets if assets is not None else []}
        self._publish(payload)

    def open_position(self, position: Position):
        with self._lock:
            # 先日切重置再累加, 避免跨午夜首笔被清零
            self._maybe_reset_daily()
            self.positions[position.symbol] = position
            self.trade_count_today += 1
            payload = {"event": "open", "symbol": position.symbol, "direction": position.direction,
                       "quantity": position.quantity, "entry_price": position.entry_price,
                       "leverage": position.leverage}
        self._publish(payload)

    def close_position(self, symbol: str, exit_price: float) -> float:
        with self._lock:
            pos = self.positions.pop(symbol, None)
            if pos is None:
                return 0.0
            # 先日切重置再累加 PnL, 避免跨午夜已实现盈亏被清零
            self._maybe_reset_daily()
            direction_mult = 1 if pos.direction == "LONG" else -1
            gross_pnl = (exit_price - pos.entry_price) * pos.quantity * direction_mult
            # 手续费 (往返 taker 0.05%×2): 开仓+平仓名义价值 × fee_rate
            fee = (pos.entry_price + exit_price) * pos.quantity * self.fee_rate
            pnl = gross_pnl - fee
            self.total_fees += fee
            self.total_equity += pnl
            self.total_realized_pnl += pnl
            self.daily_realized_pnl += pnl
            # 连亏统计: 净盈亏为正/平本不计入连亏, 仅亏损递增
            if pnl > 0:
                self.consecutive_losses = 0
            elif pnl < 0:
                self.consecutive_losses += 1
            if self.total_equity > self.peak_equity:
                self.peak_equity = self.total_equity
            payload = {"event": "close", "symbol": symbol, "direction": pos.direction,
                       "exit_price": exit_price,
                       "quantity": pos.quantity, "entry_price": pos.entry_price,
                       "gross_pnl": round(gross_pnl, 4), "fee": round(fee, 4),
                       "realized_pnl": pnl,
                       "total_equity": self.total_equity, "margin_ratio": self.margin_ratio,
                       "daily_pnl": self.daily_realized_pnl, "drawdown": self.current_drawdown}
        self._publish(payload)
        return pnl

    def add_funding_fee(self, cost: float):
        """资金费记账 (2026-08-16): 结算周期成本计入已实现盈亏。

        仅记 PnL 口径 (daily/total realized), 不改 total_equity —
        交易所结算时已从账户余额扣款, 权益以账户刷新为准。
        """
        if cost <= 0:
            return
        with self._lock:
            self._maybe_reset_daily()
            self.total_funding_fees += cost
            self.total_realized_pnl -= cost
            self.daily_realized_pnl -= cost
            payload = {"event": "funding", "cost": round(cost, 4),
                       "total_funding": round(self.total_funding_fees, 4),
                       "daily_pnl": self.daily_realized_pnl,
                       "total_equity": self.total_equity,
                       "instance": self.instance}
        self._publish(payload)

    def unrealized_pnl(self, symbol: str, mark_price: float) -> float:
        with self._lock:
            pos = self.positions.get(symbol)
        if pos is None:
            return 0.0
        direction_mult = 1 if pos.direction == "LONG" else -1
        return (mark_price - pos.entry_price) * pos.quantity * direction_mult

    @property
    def total_margin(self) -> float:
        with self._lock:
            return sum((p.quantity * p.entry_price) / p.leverage for p in self.positions.values())

    @property
    def margin_ratio(self) -> float:
        if self.total_equity <= 0:
            return 1.0
        return self.total_margin / self.total_equity

    @property
    def current_drawdown(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return (self.peak_equity - self.total_equity) / self.peak_equity

    def margin_for_symbol(self, symbol: str) -> float:
        with self._lock:
            pos = self.positions.get(symbol)
        if pos is None:
            return 0.0
        return (pos.quantity * pos.entry_price) / pos.leverage

    def margin_same_direction(self, direction: str) -> float:
        with self._lock:
            return sum((p.quantity * p.entry_price) / p.leverage
                       for p in self.positions.values() if p.direction == direction)

    def positions_snapshot(self) -> Dict[str, "Position"]:
        """锁内持仓快照 (2026-08-16: 对账/风控跨线程读, 防迭代竞态)。"""
        with self._lock:
            return dict(self.positions)

    def update_position(self, symbol: str, direction: str, quantity: float):
        """锁内更新持仓方向/数量 (对账 qty_mismatch 对齐用)。"""
        with self._lock:
            pos = self.positions.get(symbol)
            if pos is None:
                return
            pos.direction = direction
            pos.quantity = quantity

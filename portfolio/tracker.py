"""PortfolioTracker — position, equity, margin, PnL tracking."""

from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime, timezone


@dataclass
class Position:
    symbol: str
    direction: str
    quantity: float
    entry_price: float
    leverage: int
    opened_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class PortfolioTracker:
    def __init__(self, initial_equity: float = 0.0):
        self.total_equity: float = initial_equity
        self.available_balance: float = initial_equity
        self.peak_equity: float = initial_equity
        self.daily_realized_pnl: float = 0.0
        self.total_realized_pnl: float = 0.0
        self.positions: Dict[str, Position] = {}
        self.trade_count_today: int = 0
        self.consecutive_losses: int = 0
        self._last_reset_day: int = datetime.now(timezone.utc).day

    def _maybe_reset_daily(self):
        today = datetime.now(timezone.utc).day
        if today != self._last_reset_day:
            self.daily_realized_pnl = 0.0
            self.trade_count_today = 0
            self._last_reset_day = today

    def update_equity(self, total_equity: float, available_balance: Optional[float] = None):
        self.total_equity = total_equity
        if available_balance is not None:
            self.available_balance = available_balance
        if total_equity > self.peak_equity:
            self.peak_equity = total_equity

    def open_position(self, position: Position):
        self.positions[position.symbol] = position
        self.trade_count_today += 1
        self._maybe_reset_daily()

    def close_position(self, symbol: str, exit_price: float) -> float:
        pos = self.positions.pop(symbol, None)
        if pos is None:
            return 0.0
        direction_mult = 1 if pos.direction == "LONG" else -1
        pnl = (exit_price - pos.entry_price) * pos.quantity * direction_mult
        self.total_equity += pnl
        self.total_realized_pnl += pnl
        self.daily_realized_pnl += pnl
        if pnl > 0:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
        if self.total_equity > self.peak_equity:
            self.peak_equity = self.total_equity
        self._maybe_reset_daily()
        return pnl

    def unrealized_pnl(self, symbol: str, mark_price: float) -> float:
        pos = self.positions.get(symbol)
        if pos is None:
            return 0.0
        direction_mult = 1 if pos.direction == "LONG" else -1
        return (mark_price - pos.entry_price) * pos.quantity * direction_mult

    @property
    def total_margin(self) -> float:
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
        pos = self.positions.get(symbol)
        if pos is None:
            return 0.0
        return (pos.quantity * pos.entry_price) / pos.leverage

    def margin_same_direction(self, direction: str) -> float:
        return sum((p.quantity * p.entry_price) / p.leverage for p in self.positions.values() if p.direction == direction)

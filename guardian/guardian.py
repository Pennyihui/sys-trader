"""PositionGuardian — 本地价格监控与动态风控。

在 Algo Order API 条件单(安全网)之上提供策略增强:
- 跟踪止损: 价格上涨时止损跟着上移
- 动态距离: 基于 ATR 自动调整止损宽度
- 部分止盈: 达到目标价分批平仓
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from market_data.feed import MarketDataFeed
from portfolio.tracker import PortfolioTracker
from execution.order_gateway import OrderGateway, OrderRequest

logger = logging.getLogger(__name__)


@dataclass
class GuardianConfig:
    trailing_activation_pct: float = 0.003
    trailing_step_pct: float = 0.005
    atr_period: int = 14
    stop_atr_multiple: float = 2.0
    tp1_pct: float = 0.03
    tp1_ratio: float = 0.5
    tp2_pct: float = 0.06
    check_interval: float = 1.0


@dataclass
class PositionState:
    symbol: str
    direction: str
    entry_price: float
    highest_price: float
    current_stop: float
    tp1_done: bool = False
    tp2_done: bool = False


class PositionGuardian:
    """持仓守护者：监控价格、动态调整止损止盈。"""

    def __init__(
        self,
        feed: MarketDataFeed,
        portfolio: PortfolioTracker,
        gateway: OrderGateway,
        config: Optional[GuardianConfig] = None,
    ):
        self.feed = feed
        self.portfolio = portfolio
        self.gateway = gateway
        self.config = config or GuardianConfig()
        self._position_state: Dict[str, PositionState] = {}
        self._atr_cache: Dict[str, float] = {}
        self._running = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ─── ATR 计算 ───

    def _calc_atr(self, symbol: str) -> float:
        """从 KlineBuffer 计算 ATR（Average True Range）"""
        kl = self.feed.buffer.get_klines(symbol, "4h", limit=self.config.atr_period + 1)
        if len(kl) < 2:
            return 500.0
        tr_sum = 0.0
        for i in range(1, len(kl)):
            high, low = kl[i].high, kl[i].low
            prev_close = kl[i - 1].close
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_sum += tr
        return tr_sum / (len(kl) - 1)

    # ─── 初始状态 ───

    def _init_position(self, symbol: str, direction: str, entry_price: float):
        atr = self._calc_atr(symbol)
        stop_distance = max(atr * self.config.stop_atr_multiple, entry_price * 0.01)
        stop = entry_price - stop_distance if direction == "LONG" else entry_price + stop_distance
        self._position_state[symbol] = PositionState(
            symbol=symbol, direction=direction,
            entry_price=entry_price, highest_price=entry_price,
            current_stop=round(stop, 2),
        )
        self._atr_cache[symbol] = atr

    # ─── 跟踪止损 ───

    def _check_trailing(self, state: PositionState, current_price: float):
        """价格朝有利方向变动时移动止损。

        LONG:  stop = highest_price × (1 - trail_pct)
        SHORT: stop = highest_price × (1 + trail_pct)
        """
        if state.direction == "LONG":
            if current_price > state.highest_price:
                state.highest_price = current_price
            if current_price <= state.entry_price * (1 + self.config.trailing_activation_pct):
                return
            trail_pct = self._stop_pct(state.symbol)
            new_stop = round(state.highest_price * (1 - trail_pct), 2)
            min_step = round(state.highest_price * self.config.trailing_step_pct, 2)
            if new_stop > state.current_stop + min_step:
                state.current_stop = new_stop
                logger.info(f"[Guardian] {state.symbol} 跟踪止损上移 → {state.current_stop}")
        else:
            if current_price < state.highest_price:
                state.highest_price = current_price
            if current_price >= state.entry_price * (1 - self.config.trailing_activation_pct):
                return
            trail_pct = self._stop_pct(state.symbol)
            new_stop = round(state.highest_price * (1 + trail_pct), 2)
            min_step = round(state.highest_price * self.config.trailing_step_pct, 2)
            if new_stop < state.current_stop - min_step:
                state.current_stop = new_stop
                logger.info(f"[Guardian] {state.symbol} 跟踪止损下移 → {state.current_stop}")

    def _stop_pct(self, symbol: str) -> float:
        """根据 ATR 计算止损距离百分比。"""
        atr = self._atr_cache.get(symbol, 500.0)
        price = self.feed.get_last_price(symbol) or 50000.0
        return min(atr * self.config.stop_atr_multiple / price, 0.05)

    # ─── 部分止盈 ───

    def _check_tp(self, state: PositionState, current_price: float):
        entry = state.entry_price
        if state.direction == "LONG":
            pnl_pct = (current_price - entry) / entry
        else:
            pnl_pct = (entry - current_price) / entry

        pos = self.portfolio.positions.get(state.symbol)
        if not pos:
            return

        if not state.tp1_done and pnl_pct >= self.config.tp1_pct:
            qty = round(pos.quantity * self.config.tp1_ratio, 4)
            if qty > 0:
                side = "SELL" if state.direction == "LONG" else "BUY"
                req = OrderRequest(symbol=state.symbol, side=side, order_type="MARKET", quantity=qty)
                resp = self.gateway.place_order(req)
                if resp.status not in ("ERROR", "REJECTED"):
                    state.tp1_done = True
                    logger.info(f"[Guardian] TP1: {state.symbol} {qty} @ {current_price}")

        if not state.tp2_done and pnl_pct >= self.config.tp2_pct:
            remaining = 1 - self.config.tp1_ratio if not state.tp1_done else 1.0
            qty = round(pos.quantity * remaining, 4)
            if qty > 0:
                side = "SELL" if state.direction == "LONG" else "BUY"
                req = OrderRequest(symbol=state.symbol, side=side, order_type="MARKET", quantity=qty)
                resp = self.gateway.place_order(req)
                if resp.status not in ("ERROR", "REJECTED"):
                    state.tp2_done = True
                    logger.info(f"[Guardian] TP2: {state.symbol} {qty} @ {current_price}")

    # ─── 主检查循环 ───

    def _check_positions(self):
        for symbol, pos in list(self.portfolio.positions.items()):
            current_price = self.feed.get_last_price(symbol)
            if current_price is None:
                continue

            state = self._position_state.get(symbol)
            if state is None:
                self._init_position(symbol, pos.direction, pos.entry_price)
                state = self._position_state.get(symbol)
                if state is None:
                    continue

            # ATR 只在有足够K线数据时更新，否则沿用缓存
            if len(self.feed.buffer.get_klines(symbol, "4h", limit=2)) >= 2:
                self._atr_cache[symbol] = self._calc_atr(symbol)
            self._check_trailing(state, current_price)
            self._check_tp(state, current_price)

        active = set(self.portfolio.positions.keys())
        for sym in list(self._position_state.keys()):
            if sym not in active:
                del self._position_state[sym]

    # ─── 生命周期 ───

    def start(self):
        self._running = True
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("PositionGuardian started")

    def _run(self):
        while self._running and not self._stop.is_set():
            try:
                self._check_positions()
            except Exception as e:
                logger.error(f"Guardian error: {e}")
            self._stop.wait(timeout=self.config.check_interval)

    def stop(self):
        self._running = False
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        logger.info("PositionGuardian stopped")

# PositionGuardian Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development

**Goal:** 本地价格监控模块，当有持仓时跟踪价格，动态调整止损止盈。

**Architecture:** 后台线程每秒检查持仓 → 计算 ATR → 判断是否触发跟踪止损/部分止盈 → 通过 Execution Engine 下单

**Tech Stack:** Python threading, ATR计算, market_data/feed, portfolio/tracker, execution/order_gateway

---

### Task 1: PositionGuardian 核心

**Files:**
- Create: `guardian/guardian.py`
- Create: `guardian/__init__.py`
- Create: `tests/test_guardian.py`

- [ ] **Step 1: 编写测试**

`tests/test_guardian.py`:
```python
import pytest
import time
from unittest.mock import MagicMock, patch
from guardian.guardian import PositionGuardian, GuardianConfig
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker, Position


class TestPositionGuardian:
    def setup_method(self):
        self.feed = MagicMock()
        self.feed.get_last_price.return_value = 64000.0
        self.feed.get_mark_price.return_value = 64000.0
        self.tracker = PortfolioTracker(initial_equity=10000.0)
        self.gateway = MagicMock()
        self.config = GuardianConfig(check_interval=0.1)
        self.guardian = PositionGuardian(
            feed=self.feed, portfolio=self.tracker,
            gateway=self.gateway, config=self.config
        )

    def test_no_positions_does_nothing(self):
        """无持仓时 guardian 不应做任何操作"""
        self.guardian._check_positions()
        self.gateway.place_order.assert_not_called()
        self.gateway.place_algo_order.assert_not_called()

    def test_trailing_stop_activates(self):
        """价格上涨时止损应上移"""
        pos = Position(symbol="BTCUSDT", direction="LONG",
                       quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        self.guardian._position_state["BTCUSDT"] = {
            "highest_price": 60000.0,
            "current_stop": 58000.0,
            "tp1_done": False,
            "tp2_done": False,
        }
        # 价格涨到 62000
        self.feed.get_last_price.return_value = 62000.0
        # 直接注入一个初始 stop
        self.guardian._position_state["BTCUSDT"]["current_stop"] = 59000.0

        self.guardian._check_positions()

        # 止损应该上移了
        new_stop = self.guardian._position_state["BTCUSDT"]["current_stop"]
        assert new_stop > 59000.0

    def test_dynamic_stop_based_on_atr(self):
        """ATR 大时止损宽，ATR 小时止损窄"""
        pos = Position(symbol="BTCUSDT", direction="LONG",
                       quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)

        # 测试高 ATR 场景
        self.guardian._atr_cache["BTCUSDT"] = 2000.0  # 高波动
        self.guardian._check_positions()
        stop_wide = self.guardian._position_state["BTCUSDT"]["current_stop"]

        # 重置
        self.guardian._position_state = {}

        # 测试低 ATR 场景
        self.guardian._atr_cache["BTCUSDT"] = 500.0  # 低波动
        self.guardian._check_positions()
        stop_tight = self.guardian._position_state["BTCUSDT"]["current_stop"]

        # 高 ATR 时止损应该更低（距离更大）
        assert stop_wide < stop_tight

    def test_tp1_partial_close(self):
        """达到 TP1 时发 MARKET 单平 50%"""
        pos = Position(symbol="BTCUSDT", direction="LONG",
                       quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        self.guardian._position_state["BTCUSDT"] = {
            "highest_price": 60000.0,
            "current_stop": 58000.0,
            "tp1_done": False,
            "tp2_done": False,
        }

        # TP1 在 +3% = 61800
        self.feed.get_last_price.return_value = 62000.0
        self.guardian._check_positions()

        # 应该发了平仓单
        assert self.guardian._position_state["BTCUSDT"]["tp1_done"] is True
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/test_guardian.py -v
```
Expected: FAIL

- [ ] **Step 3: 实现 PositionGuardian**

`guardian/guardian.py`:
```python
"""PositionGuardian — 本地价格监控与动态风控。

在 Algo Order API 条件单(安全网)之上提供策略增强:
- 跟踪止损: 价格上涨时止损跟着上移
- 动态距离: 基于 ATR 自动调整止损宽度
- 部分止盈: 达到目标价分批平仓
"""

import math
import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional
from market_data.feed import MarketDataFeed
from portfolio.tracker import PortfolioTracker
from execution.order_gateway import OrderGateway, OrderRequest

logger = logging.getLogger(__name__)


@dataclass
class GuardianConfig:
    trailing_activation_pct: float = 0.003  # 涨 0.3% 开始跟踪
    trailing_step_pct: float = 0.005       # 每涨 0.5% 上移一次止损
    atr_period: int = 14
    stop_atr_multiple: float = 2.0         # 止损距离 = ATR × 2
    tp1_pct: float = 0.03                  # +3% 平 50%
    tp1_ratio: float = 0.5
    tp2_pct: float = 0.06                  # +6% 平剩余
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
        """从 KlineBuffer 计算 ATR"""
        kl = self.feed.buffer.get_klines(symbol, "4h", limit=self.config.atr_period + 1)
        if len(kl) < 2:
            return 500.0  # 默认值
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
        if state.direction == "LONG":
            if current_price > state.highest_price:
                state.highest_price = current_price
            activation_price = state.entry_price * (1 + self.config.trailing_activation_pct)
            if current_price <= activation_price:
                return  # 还没到激活线
            trail_distance = state.highest_price - state.current_stop
            new_stop = state.highest_price - trail_distance
            if new_stop > state.current_stop + self.config.trailing_step_pct * state.highest_price:
                state.current_stop = round(new_stop, 2)
                logger.info(f"[Guardian] {state.symbol} 跟踪止损上移: {state.current_stop}")
        else:  # SHORT
            if current_price < state.highest_price:
                state.highest_price = current_price
            activation_price = state.entry_price * (1 - self.config.trailing_activation_pct)
            if current_price >= activation_price:
                return
            trail_distance = state.current_stop - state.highest_price
            new_stop = state.highest_price + trail_distance
            if new_stop < state.current_stop - self.config.trailing_step_pct * state.highest_price:
                state.current_stop = round(new_stop, 2)
                logger.info(f"[Guardian] {state.symbol} 跟踪止损下移: {state.current_stop}")

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

        # TP1
        if not state.tp1_done and pnl_pct >= self.config.tp1_pct:
            qty = round(pos.quantity * self.config.tp1_ratio, 4)
            if qty > 0:
                side = "SELL" if state.direction == "LONG" else "BUY"
                req = OrderRequest(symbol=state.symbol, side=side, order_type="MARKET", quantity=qty)
                resp = self.gateway.place_order(req)
                if resp.status not in ("ERROR", "REJECTED"):
                    state.tp1_done = True
                    logger.info(f"[Guardian] TP1 执行: {state.symbol} {qty} @ {current_price}")

        # TP2
        if not state.tp2_done and pnl_pct >= self.config.tp2_pct:
            qty = round(pos.quantity * (1 - self.config.tp1_ratio if not state.tp1_done else 1.0), 4)
            if qty > 0:
                side = "SELL" if state.direction == "LONG" else "BUY"
                req = OrderRequest(symbol=state.symbol, side=side, order_type="MARKET", quantity=qty)
                resp = self.gateway.place_order(req)
                if resp.status not in ("ERROR", "REJECTED"):
                    state.tp2_done = True
                    logger.info(f"[Guardian] TP2 执行: {state.symbol} {qty} @ {current_price}")

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

            # 更新 ATR
            self._atr_cache[symbol] = self._calc_atr(symbol)

            self._check_trailing(state, current_price)
            self._check_tp(state, current_price)

        # 清理已平仓的持仓状态
        active_symbols = set(self.portfolio.positions.keys())
        for sym in list(self._position_state.keys()):
            if sym not in active_symbols:
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
```

- [ ] **Step 4: 创建 `__init__.py`**

```bash
touch guardian/__init__.py
```

- [ ] **Step 5: 运行测试**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/test_guardian.py -v
```
Expected: PASS (4 tests)

- [ ] **Step 6: 验证无回归**

```bash
cd D:/Documents/z_python_data_analy/Quent/Sys_trader && python -m pytest tests/ -v --tb=short
```
Expected: 86+ passed

- [ ] **Step 7: Commit**

```bash
git add guardian/ tests/test_guardian.py
git commit -m "feat: add PositionGuardian with trailing stop, ATR distance, partial TP"
```

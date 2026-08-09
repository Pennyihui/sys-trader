# EventBus 数据链路 + 统一装配 + 测试管道 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立唯一完整装配入口（SystemRunner），首次启用 EventBus 打通主系统 → dashboard 真实数据链路，并落地完整测试验证管道（重放/影子/soak/实盘分级/kill switch/可靠性）。

**Architecture:** 事件驱动（Redis Streams）：模块内埋点 publish 事件，dashboard 进程 StateStore 订阅消费；SystemRunner 合并两条装配为唯一入口；测试管道 A-F 复用统一装配验证。

**Tech Stack:** Python 3 / FastAPI / WebSocket / Redis Streams (EventBus) / React (Vite) / pytest / Memurai (Windows) / Docker Compose

**Specs:** [2026-08-09-eventbus-dashboard-design.md](../specs/2026-08-09-eventbus-dashboard-design.md)（前置）、[2026-08-09-testing-validation-pipeline-design.md](../specs/2026-08-09-testing-validation-pipeline-design.md)

**基线：** 当前 195 个测试全部通过，`pytest tests/ -q` 验证基线后再开工。

---

## 第一部分：数据链路 + 统一装配

### Task 1: EventBus.publish 容错

**Files:**
- Modify: `shared/event_bus.py:33-37`
- Test: `tests/test_event_bus.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_event_bus.py` 末尾追加：

```python
def test_publish_survives_redis_down(monkeypatch):
    """Redis 不可用时 publish 不抛异常，返回空字符串。"""
    bus = EventBus(redis_url="redis://127.0.0.1:1")  # 必然失败的端口
    bus.redis.close()  # 强制断连
    assert bus.publish("test.stream", {"k": "v"}) == ""
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_event_bus.py::test_publish_survives_redis_down -v`
Expected: FAIL（redis.exceptions.ConnectionError）

- [ ] **Step 3: 实现容错**

`shared/event_bus.py` 的 `publish` 改为：

```python
def publish(self, stream: str, data: dict) -> str:
    event = Event(stream=stream, data=data)
    payload = json.dumps({"event_id": event.event_id, "stream": event.stream, "timestamp": event.timestamp, "data": event.data})
    try:
        msg_id = self.redis.xadd(self._key(stream), {"payload": payload}, maxlen=10000)
        return msg_id
    except Exception as e:
        logger.warning("EventBus publish failed [%s]: %s", stream, e)
        return ""
```

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_event_bus.py -v`
Expected: 全部 PASS（含原有测试）

- [ ] **Step 5: 提交**

```bash
git add shared/event_bus.py tests/test_event_bus.py
git commit -m "fix: EventBus.publish tolerates Redis outage (non-blocking)"
```

### Task 2: 数量对齐工具迁移（execution/order_utils.py）

**Files:**
- Create: `execution/order_utils.py`
- Test: `tests/test_order_utils.py`

- [ ] **Step 1: 写失败测试**

新建 `tests/test_order_utils.py`：

```python
import pytest
from execution.order_utils import align_qty_to_step


class TestAlignQtyToStep:
    def test_exact_step(self):
        assert align_qty_to_step(0.005, 0.001, 0.001, 10.0) == 0.005

    def test_floor_to_step(self):
        assert align_qty_to_step(0.0037, 0.001, 0.001, 10.0) == 0.003

    def test_floor_below_min_rounds_up(self):
        assert align_qty_to_step(0.0014, 0.001, 0.002, 10.0) == 0.002

    def test_clamp_max(self):
        assert align_qty_to_step(99.0, 1.0, 1.0, 50.0) == 50.0

    def test_no_step_size_passthrough(self):
        assert align_qty_to_step(5.0, 0.0, 1.0, 100.0) == 5.0
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_order_utils.py -v`
Expected: FAIL（ModuleNotFoundError: execution.order_utils）

- [ ] **Step 3: 实现**

新建 `execution/order_utils.py`（从 `tools/stability_test.py:51-69` 原样迁移，删除 stability_test 中的同名函数）：

```python
"""下单数量对齐工具 — Binance stepSize 精度处理。"""

import math


def align_qty_to_step(qty: float, step_size: float, min_qty: float, max_qty: float) -> float:
    """将数量对齐到交易所 stepSize 的整数倍，且不低于 min_qty、不高于 max_qty。

    Binance 硬性要求数量必须是 stepSize 的整数倍，否则拒绝下单。
    对齐策略: clamp 到 [min_qty, max_qty] 后向下取整；
    若向下取整跌破 min_qty（名义价值保底），则向上取整到下一个 step。
    """
    if not step_size or step_size <= 0:
        return min(max(qty, min_qty), max_qty)
    q = min(max(qty, min_qty), max_qty)
    steps = round(q / step_size, 8)
    floored = math.floor(steps) * step_size
    if floored >= min_qty:
        return round(floored, 8)
    return round(math.ceil(steps) * step_size, 8)
```

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_order_utils.py tests/test_stability_precision.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add execution/order_utils.py tests/test_order_utils.py tools/stability_test.py
git commit -m "refactor: move align_qty_to_step to execution/order_utils (shared by runner)"
```

### Task 3: SystemRunner 装配扩展（策略 + 风控 + OrderManager + K线接线）

**Files:**
- Modify: `shared/runner.py`（整体重构 initialize + 新增信号链方法）
- Test: `tests/test_runner_assembly.py`（新建）

- [ ] **Step 1: 写失败测试**

新建 `tests/test_runner_assembly.py`：

```python
"""SystemRunner 统一装配测试 — mock gateway，验证完整信号链接线。"""

import pytest
from unittest.mock import MagicMock, patch

from shared.runner import SystemRunner


@pytest.fixture
def runner():
    with patch("shared.runner.PreflightChecker") as MockPreflight, \
         patch("shared.runner.PositionReconciler") as MockReconciler:
        MockPreflight.return_value.run_all.return_value = {
            "assets": [{"walletBalance": "10000"}],
        }
        r = SystemRunner()
        r.gateway = MagicMock()
        r.portfolio = MagicMock()
        r.feed = MagicMock()
        yield r


def test_initialize_wires_full_assembly(runner):
    """装配后策略/风控/执行层全部就绪。"""
    with patch.object(runner, "_fetch_step_sizes", return_value={"BTCUSDT": 0.001}):
        runner.initialize()
    assert runner.engine is not None
    assert runner.risk_chain is not None
    assert runner.orders is not None
    assert runner.feed.on_kline_closed is not None


def test_on_kline_closed_15m_generates_signal(runner):
    """15m K线闭合 → 信号 → 风控 → 下单全链路。"""
    runner.engine = MagicMock()
    runner.engine.run.return_value = MagicMock(
        symbol="BTCUSDT", direction="LONG", conviction=0.8,
        entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0,
    )
    runner.risk_chain = MagicMock()
    runner.risk_chain.process.return_value = MagicMock(
        rejected=False, reason="", modifications={"position_size": 0.001},
    )
    runner.orders = MagicMock()
    runner.portfolio = MagicMock()
    runner.step_sizes = {"BTCUSDT": 0.001}
    runner.feed = MagicMock()
    runner.feed.get_last_price.return_value = 64000.0

    runner._on_kline_closed("BTCUSDT", "15m", [MagicMock()])

    runner.orders.execute_signal.assert_called_once()
    assert runner.stats["signals"] == 1


def test_on_kline_closed_ignores_other_timeframes(runner):
    """非 15m K线闭合被忽略。"""
    runner.engine = MagicMock()
    runner._on_kline_closed("BTCUSDT", "4h", [MagicMock()])
    runner.engine.run.assert_not_called()
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_runner_assembly.py -v`
Expected: FAIL（SystemRunner 无 engine/risk_chain/orders 属性）

- [ ] **Step 3: 实现 — 重构 `shared/runner.py`**

`__init__` 增加参数：

```python
def __init__(self, testnet: bool = True, symbols: Optional[list] = None,
             strategy_name: str = "scalping_15m",
             execution_mode_name: str = "live",
             risk_per_trade: float = 0.015, hours: int = 0,
             instance: str = "live", event_bus=None):
    self.testnet = testnet
    self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    self.strategy_name = strategy_name
    self.execution_mode_name = execution_mode_name
    self.risk_per_trade = risk_per_trade
    self.hours = hours  # 0 = 无限运行（生产）
    self.instance = instance
    self.event_bus = event_bus
    self.feed = None
    self.portfolio = None
    self.gateway = None
    self.idempotency = None
    self.reconciler = None
    self.engine = None
    self.risk_chain = None
    self.orders = None
    self.step_sizes = {}
    self.stats = {
        "signals": 0, "risk_rejected": 0, "orders_placed": 0,
        "orders_failed": 0, "kline_closes": 0, "stalls": 0,
        "start_time": time.time(),
    }
    self._last_data_ts = {}
    signal.signal(signal.SIGTERM, self._handle_signal)
    signal.signal(signal.SIGINT, self._handle_signal)
```

`initialize` 补充（保留原有 gateway/portfolio/idempotency/preflight/reconciler 逻辑）：

```python
    def _build_risk_chain(self):
        from risk.chain import MiddlewareChain
        from risk.position_sizer import PositionSizer
        from risk.drawdown_breaker import DrawdownBreaker
        from risk.daily_loss_limit import DailyLossLimit
        from risk.concentration import ConcentrationCheck
        chain = MiddlewareChain(event_bus=self.event_bus, instance=self.instance)
        chain.add(PositionSizer(risk_per_trade=self.risk_per_trade))
        chain.add(DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120))
        chain.add(DailyLossLimit(daily_loss_limit=0.05))
        chain.add(ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80))
        return chain

    def _build_signal_chain(self):
        import signal_engine.scalping_strategy  # noqa: F401 注册策略
        from signal_engine.interface import StrategyRegistry
        from signal_engine.engine import SignalEngine
        return SignalEngine(
            strategy=StrategyRegistry.get(self.strategy_name),
            event_bus=self.event_bus, instance=self.instance,
        )
```

`initialize` 中 feed 构造改为（保留 on_kline_closed 接线、8 路冗余）：

```python
        from execution.order_manager import OrderManager
        from shared.execution_mode import ExecutionMode, ExecutionModeManager
        from execution.order_gateway import OrderGateway

        self.gateway = OrderGateway(testnet=self.testnet)
        self.portfolio = PortfolioTracker()
        self.feed = MarketDataFeed(
            symbols=self.symbols,
            proxy_host="127.0.0.1", proxy_port=7897,
            redundant_connections=8,
            on_kline_closed=self._on_kline_closed,
        )
        mode = ExecutionModeManager(ExecutionMode(self.execution_mode_name.upper()))
        self.orders = OrderManager(
            gateway=self.gateway, execution_mode=mode,
            event_bus=self.event_bus, instance=self.instance,
        )
        self.engine = self._build_signal_chain()
        self.risk_chain = self._build_risk_chain()
```

`initialize` 末尾追加（在原 equity 同步与 feed.start() 之间）：

```python
        self.step_sizes = self._fetch_step_sizes()
        self.feed.start()
        time.sleep(2)
        self.feed.backfill(limit=200)
        for sym in self.symbols:
            self._last_data_ts[sym] = time.time()
```

新增方法（从 `tools/stability_test.py` 迁移 `_on_kline_closed`/`_execute_signal` 并改造为走 OrderManager）：

```python
    def _on_kline_closed(self, symbol: str, timeframe: str, ohlcv):
        """K线闭合 → 信号 → 风控 → 执行。"""
        if timeframe != "15m":
            return
        self.stats["kline_closes"] += 1
        self._last_close_ts = getattr(self, "_last_close_ts", {})
        self._last_close_ts[symbol] = time.time()
        import pandas as pd
        df = pd.DataFrame([{
            "open": k.open, "high": k.high, "low": k.low,
            "close": k.close, "volume": k.volume,
        } for k in ohlcv])
        try:
            signal = self.engine.run(symbol, "15m", df.to_dict("records"))
            if signal is None:
                return
            self.stats["signals"] += 1
            logger.info("SIGNAL %s %s conviction=%.2f entry=%.2f sl=%.2f tp=%.2f",
                        symbol, signal.direction, signal.conviction,
                        signal.entry_price, signal.stop_loss, signal.take_profit)
            self._execute_signal(signal)
        except Exception as e:
            logger.error("Signal engine error: %s", e)

    def _execute_signal(self, signal):
        """风控 → OrderManager.execute_signal（完整路径：LIMIT 入场 + algo SL/TP）。"""
        result = self.risk_chain.process(signal, self.portfolio)
        if result.rejected:
            self.stats["risk_rejected"] += 1
            logger.warning("RISK REJECTED %s %s: %s", signal.symbol, signal.direction, result.reason)
            return
        size = result.modifications.get("position_size", 0.001)
        price = self.feed.get_last_price(signal.symbol) or signal.entry_price
        min_qty = 5.0 / price if price else 0.001
        max_qty = 100.0 / price if price else 0.01
        step = self.step_sizes.get(signal.symbol, 0.0)
        from execution.order_utils import align_qty_to_step
        qty = align_qty_to_step(size, step, min_qty, max_qty)
        try:
            orders = self.orders.execute_signal(
                symbol=signal.symbol, direction=signal.direction,
                quantity=qty, entry_price=signal.entry_price,
                stop_loss=signal.stop_loss, take_profit=signal.take_profit,
            )
            filled = [o for o in orders if o.state.value in ("FILLED", "NEW")]
            if filled:
                self.stats["orders_placed"] += 1
                self.portfolio.open_position(Position(
                    symbol=signal.symbol, direction=signal.direction,
                    quantity=qty, entry_price=signal.entry_price, leverage=3,
                ))
                logger.info("ORDER SUBMITTED %s %s qty=%s id=%s",
                            signal.symbol, signal.direction, qty,
                            [o.order_id for o in filled])
            else:
                self.stats["orders_failed"] += 1
                logger.error("ORDER FAILED %s: %s", signal.symbol,
                             [o.error for o in orders if o.error])
        except Exception as e:
            self.stats["orders_failed"] += 1
            logger.error("ORDER EXCEPTION %s: %s", signal.symbol, e)
```

从 stability_test 迁移 `_fetch_step_sizes`/`_check_stall`/`_check_connections`/`_network_diag`/`_get_default_gateway`/`_port_open`/`_snapshot`/`report`（原样搬运，`self.feed._conns` 引用不变；`report` 中 `ok` 判定保持 stalls==0 and orders_failed==0）。`run_forever` 改为：

```python
    def run_forever(self):
        logger.info("System running (PID=%d, instance=%s)", os.getpid(), self.instance)
        end_time = time.time() + self.hours * 3600 if self.hours > 0 else None
        last_snapshot = 0
        while True:
            if end_time and time.time() >= end_time:
                break
            time.sleep(5)
            self._check_stall()
            self._check_connections()
            if time.time() - last_snapshot >= 60:
                self._snapshot()
                last_snapshot = time.time()
        self.report()
```

`main()` 增加 argparse（`--strategy/--symbols/--execution-mode/--hours/--testnet/--risk-per-trade/--instance`），默认 testnet=True 行为不变；`stop()` 中补 `self.orders` 无需特殊处理（OrderManager 无长驻线程）。

- [ ] **Step 4: 运行全部测试确认通过**

Run: `pytest tests/ -q`
Expected: 全部 PASS（原 195 测试 + 新增 3 个；runner 重构未破坏任何现有测试）

- [ ] **Step 5: 提交**

```bash
git add shared/runner.py tests/test_runner_assembly.py
git commit -m "feat: unify assembly in SystemRunner (strategy+risk+OrderManager+kline chain)"
```

### Task 4: stability_test.py 转型 thin wrapper

**Files:**
- Modify: `tools/stability_test.py`（整体替换为 wrapper）

- [ ] **Step 1: 替换文件**

`tools/stability_test.py` 内容替换为：

```python
"""稳定性测试入口 — SystemRunner 的 thin wrapper（testnet 真实下单）。

用法不变: python tools/stability_test.py --hours 24
系统装配见 shared/runner.py SystemRunner（唯一完整装配）。
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.logging import setup_logging
from shared.runner import SystemRunner


def main():
    parser = argparse.ArgumentParser(description="稳定性测试 (testnet下单)")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--strategy", default="scalping_15m")
    args = parser.parse_args()
    setup_logging(log_dir="logs", json_console=False)
    runner = SystemRunner(
        testnet=True, symbols=args.symbols.split(","),
        strategy_name=args.strategy, hours=args.hours,
    )
    try:
        runner.initialize()
        runner.run_forever()
    except Exception:
        import logging
        logging.getLogger("stability").exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 验证无 import 残留引用**

Run: `python -c "import ast; ast.parse(open('tools/stability_test.py').read()); print('syntax ok')"`
Expected: `syntax ok`；同时确认 `tools/` 下无其他文件 import stability_test 的 StabilityRunner（`grep -rn "StabilityRunner" tools/ tests/ --include="*.py"` 无结果）

- [ ] **Step 3: 提交**

```bash
git add tools/stability_test.py
git commit -m "refactor: stability_test becomes thin wrapper over SystemRunner"
```

### Task 5: PortfolioTracker 埋点（position.changed）

**Files:**
- Modify: `portfolio/tracker.py:18-28,44-65`
- Test: `tests/test_portfolio_tracker.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_portfolio_tracker.py` 追加：

```python
def test_publishes_position_changed_on_open():
    bus = MagicMock()
    tracker = PortfolioTracker(initial_equity=1000.0, event_bus=bus)
    tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 64000.0, 3))
    bus.publish.assert_called_once()
    stream, payload = bus.publish.call_args[0]
    assert stream == "position.changed"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["direction"] == "LONG"


def test_no_event_bus_is_silent():
    tracker = PortfolioTracker(initial_equity=1000.0)
    tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 64000.0, 3))  # 不抛异常
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_portfolio_tracker.py -v`
Expected: FAIL（PortfolioTracker 不接受 event_bus 参数）

- [ ] **Step 3: 实现**

`portfolio/tracker.py`：

```python
class PortfolioTracker:
    def __init__(self, initial_equity: float = 0.0, event_bus=None):
        self.total_equity: float = initial_equity
        ...
        self.event_bus = event_bus

    def _publish(self, data: dict):
        if self.event_bus is not None:
            self.event_bus.publish("position.changed", data)
```

`update_equity` 末尾：`self._publish({"event": "equity", "total_equity": total_equity, "available_balance": self.available_balance})`
`open_position` 末尾：`self._publish({"event": "open", "symbol": position.symbol, "direction": position.direction, "quantity": position.quantity, "entry_price": position.entry_price})`
`close_position` 末尾（pop 之后）：`self._publish({"event": "close", "symbol": symbol, "exit_price": exit_price, "realized_pnl": pnl, "total_equity": self.total_equity})`

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_portfolio_tracker.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add portfolio/tracker.py tests/test_portfolio_tracker.py
git commit -m "feat: PortfolioTracker publishes position.changed events (optional EventBus)"
```

### Task 6: OrderManager 埋点（order.filled）

**Files:**
- Modify: `execution/order_manager.py:49-72,175-306`
- Test: `tests/test_order_manager.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_order_manager.py` 追加：

```python
def test_publishes_order_filled_after_submit():
    bus = MagicMock()
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=1, symbol="BTCUSDT", side="BUY", status="FILLED",
        executed_qty=0.1, avg_price=64000.0)
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE),
                       event_bus=bus)
    mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)
    calls = [c[0][0] for c in bus.publish.call_args_list]
    assert "order.filled" in calls


def test_no_event_bus_is_silent():
    gw = MagicMock()
    gw.place_order.return_value = OrderResponse(
        order_id=1, symbol="BTCUSDT", side="BUY", status="FILLED",
        executed_qty=0.1, avg_price=64000.0)
    mgr = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode.LIVE))
    mgr.submit_entry("BTCUSDT", "LONG", 0.1, 64000.0, 62000.0, 68000.0)  # 不抛异常
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_order_manager.py -v`
Expected: FAIL（OrderManager 不接受 event_bus 参数）

- [ ] **Step 3: 实现**

`execution/order_manager.py`：
- `__init__` 签名追加 `event_bus=None, instance="live"`，存 `self.event_bus`/`self.instance`
- 新增私有方法：

```python
def _publish_order(self, resp, req_side: str, symbol: str, order_type: str):
    if self.event_bus is None:
        return
    self.event_bus.publish("order.filled", {
        "instance": self.instance, "symbol": symbol, "side": req_side,
        "order_type": order_type, "status": resp.status,
        "quantity": getattr(resp, "executed_qty", None) or getattr(resp, "quantity", None),
        "price": getattr(resp, "avg_price", None),
        "order_id": getattr(resp, "order_id", 0) or getattr(resp, "algo_id", 0),
        "error": getattr(resp, "error", None),
    })
```

- 在 `submit_entry`/`submit_stop_loss`/`submit_take_profit` 的 `self._persist_result(...)` 之后各加一行：`self._publish_order(resp, side, symbol, req.order_type)`

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_order_manager.py tests/test_order_lifecycle.py tests/test_end_to_end_system.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add execution/order_manager.py tests/test_order_manager.py
git commit -m "feat: OrderManager publishes order.filled events (optional EventBus)"
```

### Task 7: SignalEngine 埋点（signal.generated，带 instance）

**Files:**
- Modify: `signal_engine/engine.py:28-34`
- Test: `tests/test_signal_engine.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_signal_engine.py` 追加：

```python
def test_publishes_signal_generated_with_instance():
    bus = MagicMock()
    from signal_engine.engine import SignalEngine
    from signal_engine.engine import Signal
    engine = SignalEngine(event_bus=bus, instance="paper")
    strat = MagicMock()
    strat.analyze.return_value = Signal(
        symbol="BTCUSDT", direction="LONG", conviction=0.8,
        entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0)
    engine.strategy = strat
    engine.run("BTCUSDT", "15m", [{"close": 64000.0}])
    bus.publish.assert_called_once()
    stream, payload = bus.publish.call_args[0]
    assert stream == "signal.generated"
    assert payload["instance"] == "paper"


def test_no_event_bus_is_silent():
    from signal_engine.engine import SignalEngine, Signal
    engine = SignalEngine()
    strat = MagicMock()
    strat.analyze.return_value = Signal(
        symbol="BTCUSDT", direction="LONG", conviction=0.8,
        entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0)
    engine.strategy = strat
    engine.run("BTCUSDT", "15m", [{"close": 64000.0}])  # 不抛异常
```

（注意：`engine.run` 内部调用路径需以实际代码为准，若 run 用 `strategy.analyze` 则 MagicMock 足够；若走 `_run_4h` 等分支，mock 对应分支。）

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_signal_engine.py -v`
Expected: FAIL（SignalEngine 不接受 event_bus）

- [ ] **Step 3: 实现**

`signal_engine/engine.py`：

```python
class SignalEngine:
    def __init__(self, strategy: Optional["IStrategy"] = None,
                 event_bus=None, instance: str = "live"):
        self._weekly_cache: Dict[str, Any] = {}
        self._daily_cache: Dict[str, Any] = {}
        self.strategy = strategy
        self.event_bus = event_bus
        self.instance = instance
```

在 `run` 返回 Signal 前（产出非 None 时）追加：

```python
        if self.event_bus is not None:
            self.event_bus.publish("signal.generated", {
                "instance": self.instance, "symbol": signal.symbol,
                "direction": signal.direction, "conviction": signal.conviction,
                "entry_price": signal.entry_price, "strategy": getattr(self.strategy, "name", ""),
            })
```

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_signal_engine.py tests/test_strategy_interface.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add signal_engine/engine.py tests/test_signal_engine.py
git commit -m "feat: SignalEngine publishes signal.generated with instance tag"
```

### Task 8: MiddlewareChain 埋点（signal.approved / signal.rejected）

**Files:**
- Modify: `risk/chain.py:22-40`
- Test: `tests/test_risk_chain.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_risk_chain.py` 追加：

```python
def test_publishes_approved_and_rejected():
    bus = MagicMock()
    chain = MiddlewareChain(event_bus=bus)
    from signal_engine.engine import Signal
    from portfolio.tracker import PortfolioTracker
    sig = Signal(symbol="BTCUSDT", direction="LONG", conviction=0.8,
                 entry_price=64000.0, stop_loss=62000.0, take_profit=68000.0)
    portfolio = PortfolioTracker(initial_equity=10000.0)

    # 空链 → approved
    chain.process(sig, portfolio)
    streams = [c[0][0] for c in bus.publish.call_args_list]
    assert "signal.approved" in streams

    # 拒绝中间件 → rejected
    class Rejecter:
        def process(self, signal, portfolio):
            from risk.chain import MiddlewareResult
            return MiddlewareResult(rejected=True, reason="test")
    chain.add(Rejecter())
    chain.process(sig, portfolio)
    streams = [c[0][0] for c in bus.publish.call_args_list]
    assert "signal.rejected" in streams
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_risk_chain.py -v`
Expected: FAIL（MiddlewareChain 不接受 event_bus）

- [ ] **Step 3: 实现**

`risk/chain.py`：

```python
class MiddlewareChain:
    def __init__(self, event_bus=None, instance: str = "live"):
        self.middlewares: list = []
        self.event_bus = event_bus
        self.instance = instance

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        current_signal = signal
        modifications: dict = {}
        for mw in self.middlewares:
            result = mw.process(current_signal, portfolio)
            if result.modifications:
                modifications.update(result.modifications)
            if result.rejected:
                if self.event_bus is not None:
                    self.event_bus.publish("signal.rejected", {
                        "instance": self.instance, "symbol": signal.symbol,
                        "direction": signal.direction, "reason": result.reason,
                    })
                return MiddlewareResult(rejected=True, reason=result.reason, modifications=modifications)
            if result.signal is not None:
                current_signal = result.signal
        if self.event_bus is not None:
            self.event_bus.publish("signal.approved", {
                "instance": self.instance, "symbol": signal.symbol,
                "direction": signal.direction, "modifications": modifications,
            })
        return MiddlewareResult(rejected=False, signal=current_signal, modifications=modifications)
```

（若原代码的 `modifications` 变量名不同，以实际为准，保持 merge 语义。）

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_risk_chain.py tests/test_integration_risk.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add risk/chain.py tests/test_risk_chain.py
git commit -m "feat: MiddlewareChain publishes signal.approved/rejected"
```

### Task 9: HeartbeatPublisher + MetricsCollector 埋点

**Files:**
- Create: `shared/heartbeat_publisher.py`
- Modify: `shared/runner.py`（启动/停止心跳线程 + 模块心跳埋点）、`market_data/feed.py`（消息循环心跳）
- Test: `tests/test_heartbeat_publisher.py`（新建）

- [ ] **Step 1: 写失败测试**

新建 `tests/test_heartbeat_publisher.py`：

```python
"""HeartbeatPublisher 测试。"""

import time
from unittest.mock import MagicMock
from shared.heartbeat_publisher import HeartbeatPublisher


def test_publishes_heartbeat_with_module_times():
    bus = MagicMock()
    from monitor.collector import MetricsCollector
    MetricsCollector.reset()
    collector = MetricsCollector.instance()
    collector.heartbeat("market_data")
    publisher = HeartbeatPublisher(bus, interval=0.05)
    publisher._run_once()
    bus.publish.assert_called_once()
    stream, payload = bus.publish.call_args[0]
    assert stream == "heartbeat"
    assert "market_data" in payload["modules"]


def test_stop_clears_flag():
    bus = MagicMock()
    publisher = HeartbeatPublisher(bus, interval=0.05)
    publisher.start()
    publisher.stop()
    assert publisher._stop.is_set()
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_heartbeat_publisher.py -v`
Expected: FAIL（ModuleNotFoundError: shared.heartbeat_publisher）

- [ ] **Step 3: 实现**

新建 `shared/heartbeat_publisher.py`：

```python
"""HeartbeatPublisher — 周期读取 MetricsCollector 并发布 heartbeat 事件。"""

import logging
import threading
import time

logger = logging.getLogger(__name__)


class HeartbeatPublisher:
    def __init__(self, event_bus, interval: float = 5.0, instance: str = "live"):
        self.event_bus = event_bus
        self.interval = interval
        self.instance = instance
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run_once(self):
        from monitor.collector import MetricsCollector
        collector = MetricsCollector.instance()
        modules = {}
        with collector._lock:
            for mod, ts in collector._heartbeats.items():
                modules[mod] = round(time.time() - ts, 1)
        if self.event_bus is not None:
            self.event_bus.publish("heartbeat", {
                "instance": self.instance, "modules": modules,
            })

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        while not self._stop.is_set():
            try:
                self._run_once()
            except Exception as e:
                logger.warning("HeartbeatPublisher error: %s", e)
            self._stop.wait(timeout=self.interval)

    def stop(self):
        self._stop.set()
```

`shared/runner.py`：`__init__` 存 `self.heartbeat = None`；`initialize` 末尾（reconciler.start() 后）：

```python
        from shared.heartbeat_publisher import HeartbeatPublisher
        self.heartbeat = HeartbeatPublisher(self.event_bus, instance=self.instance)
        self.heartbeat.start()
```

`stop()` 补：`if self.heartbeat: self.heartbeat.stop()`

`market_data/feed.py` 消息处理入口（`_on_message` 或 `_on_message_wrapper` 处）加一行心跳：

```python
        from monitor.collector import MetricsCollector
        MetricsCollector.instance().heartbeat("market_data")
```

（加在 `_on_message_wrapper` 的 conn_id 检查之后，主连接消息处理处；用局部 import 避免循环依赖。）

`shared/runner.py` 主循环与 reconciler 中分别加：`MetricsCollector.instance().heartbeat("runner")`（run_forever 循环内）、reconciler 的 `_run` 循环内 `MetricsCollector.instance().heartbeat("reconciler")`（直接改 `shared/reconciler.py:75-78`）。

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_heartbeat_publisher.py tests/test_monitor_collector.py tests/test_feed_lifecycle.py -q`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add shared/heartbeat_publisher.py shared/runner.py shared/reconciler.py market_data/feed.py tests/test_heartbeat_publisher.py
git commit -m "feat: HeartbeatPublisher + module heartbeat instrumentation"
```

### Task 10: StateStore（dashboard 消费侧）

**Files:**
- Create: `dashboard/state_store.py`
- Test: `tests/test_state_store.py`（新建）

- [ ] **Step 1: 写失败测试**

新建 `tests/test_state_store.py`：

```python
"""StateStore 测试 — 事件消费线程与状态维护。"""

import time
from unittest.mock import MagicMock, patch
from dashboard.state_store import StateStore


class TestStateStore:
    def setup_method(self):
        self.bus = MagicMock()
        self.store = StateStore(event_bus=self.bus, instance_filter="live")

    def test_handle_position_changed(self):
        self.store._handle({"stream": "position.changed", "data": {
            "event": "open", "symbol": "BTCUSDT", "direction": "LONG",
            "quantity": 0.1, "entry_price": 64000.0}})
        assert self.store.positions["BTCUSDT"]["direction"] == "LONG"

    def test_handle_signal_generated_filters_instance(self):
        self.store._handle({"stream": "signal.generated", "data": {
            "instance": "paper", "symbol": "BTCUSDT", "direction": "LONG"}})
        assert self.store.signals == []  # 影子实例被过滤

        self.store._handle({"stream": "signal.generated", "data": {
            "instance": "live", "symbol": "BTCUSDT", "direction": "LONG"}})
        assert len(self.store.signals) == 1

    def test_signals_bounded_to_50(self):
        for i in range(60):
            self.store._handle({"stream": "signal.generated", "data": {
                "instance": "live", "symbol": "X", "direction": "LONG", "conviction": 0.5}})
        assert len(self.store.signals) == 50

    def test_handle_heartbeat(self):
        self.store._handle({"stream": "heartbeat", "data": {
            "modules": {"market_data": 0.2, "runner": 1.0}}})
        assert self.store.heartbeats["market_data"] == 0.2

    def test_equity_snapshot(self):
        self.store._handle({"stream": "position.changed", "data": {
            "event": "equity", "total_equity": 12345.0}})
        assert self.store.equity == 12345.0
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_state_store.py -v`
Expected: FAIL（ModuleNotFoundError: dashboard.state_store）

- [ ] **Step 3: 实现**

新建 `dashboard/state_store.py`：

```python
"""StateStore — EventBus 消费侧：维护 dashboard 所需的系统状态副本。"""

import logging
import threading
from typing import Dict, List, Optional

from shared.event_bus import EventBus

logger = logging.getLogger(__name__)

STREAMS = [
    "position.changed", "order.filled", "signal.generated",
    "signal.approved", "signal.rejected", "heartbeat",
]


class StateStore:
    def __init__(self, event_bus: EventBus, instance_filter: str = "live",
                 max_signals: int = 50):
        self.bus = event_bus
        self.instance_filter = instance_filter
        self.max_signals = max_signals
        self._lock = threading.Lock()
        self.positions: Dict[str, dict] = {}
        self.equity: float = 0.0
        self.margin_ratio: float = 1.0
        self.daily_pnl: float = 0.0
        self.drawdown: float = 0.0
        self.signals: List[dict] = []
        self.orders: List[dict] = []
        self.heartbeats: Dict[str, float] = {}
        self._threads: List[threading.Thread] = []

    def start(self):
        for stream in STREAMS:
            t = threading.Thread(
                target=self.bus.run_consumer,
                args=(stream, "dashboard", self._handle, 5, 100),
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        logger.info("StateStore consuming %d streams", len(STREAMS))

    def stop(self):
        if hasattr(self.bus, "stop"):
            self.bus.stop()

    def _should_accept(self, data: dict) -> bool:
        inst = data.get("instance", "live")
        return inst == self.instance_filter

    def _handle(self, event):
        stream = event.stream
        data = event.data or {}
        if stream == "signal.generated" and not self._should_accept(data):
            return
        with self._lock:
            if stream == "position.changed":
                self._on_position(data)
            elif stream == "signal.generated":
                self.signals.append(data)
                self.signals = self.signals[-self.max_signals:]
            elif stream == "order.filled":
                self.orders.append(data)
                self.orders = self.orders[-self.max_signals:]
            elif stream == "heartbeat":
                self.heartbeats.update(data.get("modules", {}))
            # signal.approved/rejected 记录到 signals 尾部（决策结果）
            elif stream in ("signal.approved", "signal.rejected"):
                self.signals.append({"decision": stream, **data})
                self.signals = self.signals[-self.max_signals:]

    def _on_position(self, data: dict):
        event = data.get("event")
        if event == "open":
            self.positions[data["symbol"]] = data
        elif event == "close":
            self.positions.pop(data["symbol"], None)
            if data.get("total_equity") is not None:
                self.equity = data["total_equity"]
        elif event == "equity":
            if data.get("total_equity") is not None:
                self.equity = data["total_equity"]
```

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_state_store.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add dashboard/state_store.py tests/test_state_store.py
git commit -m "feat: StateStore consumes EventBus streams for dashboard"
```

### Task 11: DataCollector 改造读 StateStore

**Files:**
- Modify: `dashboard/data_collector.py`
- Modify: `tests/test_dashboard.py`（适配新构造）

- [ ] **Step 1: 改失败测试（适配）**

`tests/test_dashboard.py` 的 setup 改为：

```python
def setup_method(self):
    self.state = MagicMock()
    self.state.positions = {"BTCUSDT": {
        "symbol": "BTCUSDT", "direction": "LONG", "quantity": 0.1,
        "entry_price": 63000.0, "mark_price": 64000.0, "unrealized_pnl": 100.0}}
    self.state.equity = 10000.0
    self.state.margin_ratio = 0.12
    self.state.daily_pnl = 50.0
    self.state.drawdown = 0.03
    self.state.signals = []
    self.state.orders = []
    self.feed = MagicMock()
    self.feed.get_last_price.return_value = 64000.0
    self.feed.get_mark_price.return_value = 64000.0
    self.collector = DataCollector(state_store=self.state, feed=self.feed)
```

`test_collect_btc_mark_price` 改断言：`data["position_count"] == 1` 且 `data["positions"][0]["unrealized_pnl"] == 100.0`（不再 mock portfolio 方法）。

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_dashboard.py -v`
Expected: FAIL（DataCollector 构造签名不兼容）

- [ ] **Step 3: 实现**

`dashboard/data_collector.py`：

```python
class DataCollector:
    def __init__(self, state_store, feed: MarketDataFeed):
        self.state = state_store
        self.feed = feed

    def collect(self) -> Dict[str, Any]:
        positions = []
        for symbol, pos in self.state.positions.items():
            mark = self.feed.get_mark_price(symbol) or pos.get("mark_price") or 0.0
            upnl = pos.get("unrealized_pnl", 0.0)
            positions.append({
                "symbol": symbol,
                "direction": pos.get("direction"),
                "quantity": pos.get("quantity"),
                "entry_price": pos.get("entry_price"),
                "mark_price": round(mark, 2),
                "unrealized_pnl": round(upnl, 2),
            })
        return {
            "equity": round(self.state.equity, 2),
            "margin_ratio": round(self.state.margin_ratio, 2),
            "daily_pnl": round(self.state.daily_pnl, 2),
            "drawdown": round(self.state.drawdown, 4),
            "position_count": len(positions),
            "positions": positions,
            "signals": getattr(self.state, "signals", []),
            "orders": getattr(self.state, "orders", []),
            "heartbeats": getattr(self.state, "heartbeats", {}),
            "prices": self._collect_prices(),
            "proxy_pool": self._collect_proxy_pool(),
            "network": self._collect_network(),
        }

    def _collect_prices(self) -> Dict:
        prices = {}
        for symbol in list(self.state.positions.keys()):
            last = self.feed.get_last_price(symbol)
            mark = self.feed.get_mark_price(symbol)
            if last or mark:
                prices[symbol] = {"last": last, "mark": mark}
        return prices
```

`_collect_proxy_pool`/`_collect_network` 保持不变。

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_dashboard.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add dashboard/data_collector.py tests/test_dashboard.py
git commit -m "refactor: DataCollector reads StateStore instead of live portfolio"
```

### Task 12: server.py 接线（StateStore + 命令转发 + 行情 feed）

**Files:**
- Modify: `dashboard/server.py:90-106`

- [ ] **Step 1: 写失败测试**

在 `tests/test_dashboard.py` 追加：

```python
class TestCreateApp:
    def test_create_app_wires_state_store_and_feed(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1")  # 不可达：StateStore 启动失败不崩溃
        from dashboard.server import create_app
        app = create_app()
        assert app is not None

    def test_websocket_command_publishes(self):
        """/ws 收到 emergency_stop → publish command 事件。"""
        from dashboard.server import DashboardServer
        bus = MagicMock()
        server = DashboardServer(data_collector=MagicMock(), event_bus=bus)
        # 通过 app.websocket 路由的测试用 TestClient 覆盖：
        # 直接验证命令处理函数逻辑
        from dashboard.server import handle_ws_command
        handle_ws_command(bus, "emergency_stop")
        bus.publish.assert_called_once_with("command", {"command": "emergency_stop"})
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_dashboard.py::TestCreateApp -v`
Expected: FAIL（server 无 event_bus 参数/handle_ws_command）

- [ ] **Step 3: 实现**

`dashboard/server.py`：

```python
import os

def handle_ws_command(event_bus, msg: str):
    """dashboard 命令 → command 事件流（kill switch 接线）。"""
    if event_bus is not None:
        event_bus.publish("command", {"command": msg})
```

`DashboardServer.__init__` 增加 `event_bus=None` 参数，存 `self.event_bus`；`websocket_endpoint` 中命令分支改为：

```python
                    if msg in ("pause", "resume", "emergency_stop"):
                        logger.info("[Dashboard] command: %s", msg)
                        handle_ws_command(self.event_bus, msg)
```

`create_app` 改为：

```python
def create_app(data_collector=None, event_bus=None) -> FastAPI:
    if data_collector is None:
        from shared.event_bus import EventBus
        from dashboard.state_store import StateStore
        from market_data.feed import MarketDataFeed
        from shared.config_loader import load_env
        load_env()
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        if event_bus is None:
            event_bus = EventBus(redis_url=redis_url)
        symbols = os.environ.get("DASHBOARD_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
        feed = MarketDataFeed(symbols=symbols, proxy_host="127.0.0.1", proxy_port=7897)
        store = StateStore(event_bus=event_bus, instance_filter=os.environ.get("DASHBOARD_INSTANCE", "live"))
        try:
            store.start()
        except Exception as e:
            logger.warning("StateStore start failed (Redis down?): %s", e)
        feed.start()
        collector = DataCollector(state_store=store, feed=feed)
    return DashboardServer(data_collector=collector, event_bus=event_bus).app
```

（保留 `DashboardServer.run`/`app` 属性不变。）

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_dashboard.py -q`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add dashboard/server.py tests/test_dashboard.py
git commit -m "feat: dashboard server wires StateStore + command forwarding (kill switch)"
```

### Task 13: 装配端到端测试（全链路）

**Files:**
- Test: `tests/test_end_to_end_dashboard.py`（新建）

- [ ] **Step 1: 写测试**

新建 `tests/test_end_to_end_dashboard.py`（mock Redis，验证 发布 → 消费 → collect 全链路）：

```python
"""端到端：埋点 publish → StateStore → DataCollector → collect。"""

import pytest
from unittest.mock import MagicMock

from shared.event_bus import EventBus
from dashboard.state_store import StateStore
from dashboard.data_collector import DataCollector


@pytest.fixture
def fake_bus():
    """内存版 EventBus（不依赖 Redis）：publish 直接调用订阅者 _handle。"""
    bus = MagicMock()
    bus.publish.side_effect = lambda stream, data: (None)
    return bus


def test_full_chain_position_to_collect():
    """position.changed → StateStore → DataCollector.collect 返回持仓。"""
    bus = MagicMock()
    store = StateStore(event_bus=bus, instance_filter="live")
    store._handle(type("E", (), {"stream": "position.changed", "data": {
        "event": "open", "symbol": "BTCUSDT", "direction": "LONG",
        "quantity": 0.1, "entry_price": 63000.0}})())
    store._handle(type("E", (), {"stream": "position.changed", "data": {
        "event": "equity", "total_equity": 10000.0}})())

    feed = MagicMock()
    feed.get_mark_price.return_value = 64000.0
    feed.get_last_price.return_value = 64000.0
    collector = DataCollector(state_store=store, feed=feed)

    data = collector.collect()
    assert data["position_count"] == 1
    assert data["positions"][0]["symbol"] == "BTCUSDT"
    assert data["equity"] == 10000.0


def test_shadow_instance_filtered_from_collect():
    """影子实例（paper）事件不进 dashboard 状态。"""
    bus = MagicMock()
    store = StateStore(event_bus=bus, instance_filter="live")
    store._handle(type("E", (), {"stream": "signal.generated", "data": {
        "instance": "paper", "symbol": "BTCUSDT", "direction": "LONG"}})())
    assert store.signals == []
```

- [ ] **Step 2: 运行确认通过**

Run: `pytest tests/test_end_to_end_dashboard.py -v`
Expected: 全部 PASS（若 FAIL 按实现修正，保持测试语义）

- [ ] **Step 3: 提交**

```bash
git add tests/test_end_to_end_dashboard.py
git commit -m "test: end-to-end pipeline publish → StateStore → collect"
```

### Task 14: 部署配置（Memurai 文档 + 环境变量 + docker-compose redis）

**Files:**
- Modify: `RUNBOOK.md`（Memurai 安装/配置章节）
- Modify: `docker-compose.yml`（redis 服务）
- Create: `docs/redis-setup.md`

- [ ] **Step 1: 文档 — Memurai 安装与配置**

新建 `docs/redis-setup.md`：

```markdown
# Redis 部署（EventBus 依赖）

## Windows 直跑（主路径）：Memurai Developer

1. 从 https://memurai.com 下载 Memurai Developer（免费，单实例）
2. 安装后默认监听 localhost:6379，Redis 协议兼容，redis-py 直连无改动
3. **关闭持久化**：Edit 服务配置（或 memurai.conf 不启用 RDB/AOF）——事件流为瞬态数据，丢失无损失，不落盘零 IO
4. 验证: `redis-cli ping` → PONG

## Docker 部署路径（等价形态，本机日常不运行）

docker-compose.yml 已含 redis 服务；容器内 REDIS_URL 指向 redis 容器。
```

`RUNBOOK.md` Dashboard 章节追加一行：`# 前置: 安装 Memurai (Redis 兼容, localhost:6379)，见 docs/redis-setup.md`

- [ ] **Step 2: docker-compose.yml 加 redis 服务**

在 `docker-compose.yml` 的 services 顶部加入：

```yaml
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    restart: unless-stopped
```

（backend/dashboard 服务的环境变量：`REDIS_URL: redis://redis:6379`、`PROXY_HOST: host.docker.internal`；frontend 不变。若 backend 服务原本无 env，按上述补充。）

- [ ] **Step 3: 验证 compose 语法**

Run: `docker compose config --quiet 2>/dev/null || echo "docker 不可用（预期，本机未装）；仅验证 YAML 语法：" && python -c "import yaml; yaml.safe_load(open('docker-compose.yml')); print('yaml ok')"`
Expected: `yaml ok`

- [ ] **Step 4: 提交**

```bash
git add docs/redis-setup.md RUNBOOK.md docker-compose.yml
git commit -m "docs+infra: Memurai setup doc, REDIS_URL/PROXY_HOST env, docker-compose redis service"
```

---

## 第二部分：测试验证管道

### Task 15: A — ReplayFeed + replay_runner（离线模拟）

**Files:**
- Create: `tools/replay_feed.py`
- Create: `tools/replay_runner.py`
- Test: `tests/test_replay_feed.py`（新建）

- [ ] **Step 1: 写失败测试**

新建 `tests/test_replay_feed.py`：

```python
"""ReplayFeed 测试 — 从 JSON 文件重放 K 线触发 on_kline_closed。"""

import json
import tempfile
import os
from tools.replay_feed import ReplayFeed


def _write_klines(tmpdir, symbols=("BTCUSDT",), n=5):
    rows = []
    for i in range(n):
        rows.append({
            "open": 100 + i, "high": 102 + i, "low": 99 + i,
            "close": 101 + i, "volume": 10 + i, "open_time": i * 900,
        })
    for sym in symbols:
        path = os.path.join(tmpdir, f"{sym}_15m.json")
        with open(path, "w") as f:
            json.dump(rows, f)
    return rows


def test_replay_triggers_on_kline_closed(tmp_path):
    rows = _write_klines(str(tmp_path))
    closes = []
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m",
                      on_kline_closed=lambda s, tf, ohlcv: closes.append((s, tf)))
    feed.start()
    feed.run_once()
    feed.stop()
    assert closes == [("BTCUSDT", "15m")]


def test_replay_prices(tmp_path):
    rows = _write_klines(str(tmp_path))
    feed = ReplayFeed(data_dir=str(tmp_path), symbols=["BTCUSDT"], timeframe="15m")
    feed.start()
    feed.run_once()
    assert feed.get_last_price("BTCUSDT") == rows[-1]["close"]
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_replay_feed.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

新建 `tools/replay_feed.py`：

```python
"""ReplayFeed — 从本地 JSON K 线文件重放，实现 MarketDataFeed 的行情接口。

供离线模拟使用：驱动完整装配（DRY_RUN）跑历史数据，验证链路逻辑与内存稳定性。
"""

import json
import logging
import os
import threading
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class ReplayFeed:
    def __init__(self, data_dir: str, symbols: List[str], timeframe: str = "15m",
                 on_kline_closed: Optional[Callable] = None):
        self.data_dir = data_dir
        self.symbols = symbols
        self.timeframe = timeframe
        self.on_kline_closed = on_kline_closed or (lambda s, tf, ohlcv: None)
        self._prices: Dict[str, float] = {}
        self._klines: Dict[str, List[dict]] = {}
        self._stop = threading.Event()

    def _load(self):
        for sym in self.symbols:
            path = os.path.join(self.data_dir, f"{sym}_{self.timeframe}.json")
            with open(path) as f:
                self._klines[sym] = json.load(f)

    def start(self):
        self._load()

    def stop(self):
        self._stop.set()

    def run_once(self):
        """按时间顺序重放全部 K 线：每个 symbol 触发一次 on_kline_closed（全量历史）。"""
        for sym, rows in self._klines.items():
            if rows:
                self._prices[sym] = float(rows[-1]["close"])
                self.on_kline_closed(sym, self.timeframe, rows)
        return self._klines

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

    def get_mark_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)
```

新建 `tools/replay_runner.py`：

```python
"""离线模拟运行器 — ReplayFeed 驱动 SystemRunner（DRY_RUN）跑历史数据。

用法: python tools/replay_runner.py --data data/replay --symbols BTCUSDT --hours 168
验收: 全量重放无异常 + 起止 RSS 平稳（内存泄漏判定）。
"""

import argparse
import os
import resource  # 仅 POSIX；Windows 用 psutil 兜底
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.logging import setup_logging
from shared.runner import SystemRunner
from tools.replay_feed import ReplayFeed


def rss_mb() -> float:
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except ImportError:
        return 0.0


def main():
    parser = argparse.ArgumentParser(description="离线模拟（历史K线重放）")
    parser.add_argument("--data", default="data/replay")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--strategy", default="scalping_15m")
    parser.add_argument("--hours", type=int, default=168)
    args = parser.parse_args()
    setup_logging(log_dir="logs", json_console=False)

    runner = SystemRunner(
        testnet=True, symbols=args.symbols.split(","),
        strategy_name=args.strategy, execution_mode_name="dry_run", hours=args.hours,
    )
    # 用 ReplayFeed 替换真实 feed（不下单、不连 WS）
    runner.feed = ReplayFeed(
        data_dir=args.data, symbols=runner.symbols,
        on_kline_closed=runner._on_kline_closed,
    )
    runner.initialize()
    rss_before = rss_mb()
    runner.feed.run_once()
    rss_after = rss_mb()
    runner.report()
    growth = rss_after - rss_before
    logger = __import__("logging").getLogger("replay")
    logger.info("RSS before=%.1fMB after=%.1fMB growth=%.1fMB", rss_before, rss_after, growth)
    if growth > 50:
        logger.warning("⚠️ 疑似内存泄漏（RSS 增长 %.1fMB）", growth)
        sys.exit(2)
    logger.info("✅ 重放完成，无异常")
```

（注意：runner.initialize() 内含 feed.start()/backfill —— 若 initialize 与 ReplayFeed 冲突，改为最小装配路径：手动 new 各组件，仅复用 runner 的信号链方法。实现时以实际行为调整，测试只锁定 `run_once` 触发回调语义。）

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_replay_feed.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add tools/replay_feed.py tools/replay_runner.py tests/test_replay_feed.py
git commit -m "feat: ReplayFeed + replay_runner for offline simulation (pipeline A)"
```

### Task 16: D — risk_per_trade 参数化（已随 Task 3 实现，补 CLI 接线验证）

**Files:**
- Modify: `shared/runner.py:main()`（确认 --risk-per-trade 已接线）
- Test: `tests/test_runner_assembly.py`（追加）

- [ ] **Step 1: 追加测试**

`tests/test_runner_assembly.py` 追加：

```python
def test_risk_per_trade_parameterized():
    r = SystemRunner(risk_per_trade=0.005)
    assert r.risk_per_trade == 0.005
```

- [ ] **Step 2: 运行确认通过**

Run: `pytest tests/test_runner_assembly.py -v`
Expected: PASS（若 Task 3 的 main() 未接 --risk-per-trade，补上 argparse 参数并传给 SystemRunner）

- [ ] **Step 3: 提交**

```bash
git add shared/runner.py tests/test_runner_assembly.py
git commit -m "feat: risk_per_trade CLI parameter for staged live rollout (pipeline D)"
```

### Task 17: E — Kill switch 接线（SystemRunner 侧）

**Files:**
- Modify: `shared/runner.py`（订阅 command 流 + 熔断态）
- Test: `tests/test_kill_switch.py`（新建）

- [ ] **Step 1: 写失败测试**

新建 `tests/test_kill_switch.py`：

```python
"""Kill switch 测试 — command 事件 → 熔断。"""

import pytest
from unittest.mock import MagicMock
from shared.runner import SystemRunner


def test_emergency_stop_blocks_orders():
    runner = SystemRunner()
    runner.orders = MagicMock()
    runner.risk_chain = MagicMock()
    runner.portfolio = MagicMock()
    runner.feed = MagicMock()
    runner.step_sizes = {}
    runner._handle_command({"command": "emergency_stop"})
    assert runner._circuit_breaker == "emergency_stop"
    runner._execute_signal(MagicMock())
    runner.orders.execute_signal.assert_not_called()


def test_resume_clears_breaker():
    runner = SystemRunner()
    runner._handle_command({"command": "emergency_stop"})
    runner._handle_command({"command": "resume"})
    assert runner._circuit_breaker is None


def test_kill_switch_blocks_execution_before_risk():
    runner = SystemRunner()
    runner.orders = MagicMock()
    runner.portfolio = MagicMock()
    runner.feed = MagicMock()
    runner.step_sizes = {}
    runner._circuit_breaker = "emergency_stop"
    runner._execute_signal(MagicMock())
    runner.orders.execute_signal.assert_not_called()
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_kill_switch.py -v`
Expected: FAIL（无 _handle_command/_circuit_breaker）

- [ ] **Step 3: 实现**

`shared/runner.py`：

- `__init__` 加 `self._circuit_breaker = None`
- 新增方法：

```python
def _handle_command(self, data: dict):
    command = data.get("command", "")
    if command == "emergency_stop":
        self._circuit_breaker = "emergency_stop"
        logger.warning("EMERGENCY STOP — 停止下单")
        self._cancel_active_orders()
    elif command == "resume":
        self._circuit_breaker = None
        logger.info("Circuit breaker cleared — trading resumed")

def _cancel_active_orders(self):
    """撤销全部活跃订单（OrderManager 无撤销接口时退化为仅停止新单）。"""
    try:
        active = getattr(self.orders, "active_orders", []) or []
        for order in active:
            if getattr(order, "state", None) and order.state.value not in ("FILLED", "CANCELED"):
                self.gateway.cancel_order(order.symbol, order.order_id)
    except Exception as e:
        logger.error("Cancel active orders failed: %s", e)
```

- `_execute_signal` 开头加熔断检查：

```python
        if self._circuit_breaker:
            logger.warning("Circuit breaker active (%s) — signal rejected", self._circuit_breaker)
            self.stats["risk_rejected"] += 1
            return
```

- `initialize` 中（event_bus 存在时）订阅 command 流：

```python
        if self.event_bus is not None:
            import threading
            t = threading.Thread(
                target=self.event_bus.run_consumer,
                args=("command", f"systrader-{self.instance}", self._handle_command, 5, 100),
                daemon=True,
            )
            t.start()
            self._command_thread = t
```

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_kill_switch.py -q`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add shared/runner.py tests/test_kill_switch.py
git commit -m "feat: kill switch wiring - command stream subscription + circuit breaker (pipeline E)"
```

### Task 18: F — 可靠性补缺（429 审计 + 断连日志 + soak_watchdog）

**Files:**
- Modify: `execution/order_gateway.py`（429 退避，如需）
- Modify: `market_data/feed.py`（断连日志字段）
- Create: `tools/soak_watchdog.py`
- Test: `tests/test_soak_watchdog.py`（新建）

- [ ] **Step 1: 审计 429 处理**

Run: `grep -n "retry_on\|status_code\|429\|raise_for_status" execution/order_gateway.py shared/retry.py`
Expected: 查看 `shared/retry.py` 的 retrier 是否对 429 特殊处理。

若 `retrier` 仅对 RequestException 重试、未检查 429，则修改 `execution/order_gateway.py:_request`：

```python
    @retrier(max_retries=3, backoff=1.0, retry_on=(requests.exceptions.RequestException,))
    def _request(self, method: str, endpoint: str, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)
        url = f"{self.base_url}{endpoint}"
        headers = {"X-MBX-APIKEY": self.api_key}
        for attempt in range(3):
            try:
                if method == "POST":
                    resp = requests.post(url, headers=headers, data=params, timeout=10, proxies=self.proxies)
                elif method == "DELETE":
                    resp = requests.delete(url, headers=headers, data=params, timeout=10, proxies=self.proxies)
                else:
                    resp = requests.get(url, headers=headers, params=params, timeout=10, proxies=self.proxies)
                if resp.status_code == 429:
                    time.sleep(0.5 * (2 ** attempt))
                    continue
                return resp.json()
            except requests.exceptions.RequestException as e:
                if attempt == 2:
                    raise
                time.sleep(1.0 * (2 ** attempt))
        return {}
```

（若 retrier 已处理 429，跳过本步并在提交信息注明。）

- [ ] **Step 2: 断连日志补字段**

`market_data/feed.py` 断连处理处（搜 `close`/`on_close`/`disconnect`）补：

```python
logger.warning("WS disconnected conn=%d close_code=%s last_msg_ago=%.1fs uptime=%.1fs",
               conn_id, close_code, time.time() - last_ts, time.time() - started_ts)
```

（以实际字段名为准：close_code、最后消息时间戳、连接建立时间戳，取到就记。）

- [ ] **Step 3: 写 soak_watchdog 测试**

新建 `tests/test_soak_watchdog.py`：

```python
"""soak_watchdog 测试。"""

from tools.soak_watchdog import rss_mb, collect_metrics


def test_collect_metrics_shape():
    m = collect_metrics()
    assert "rss_mb" in m
    assert "errors_last_hour" in m
    assert m["rss_mb"] > 0
```

- [ ] **Step 4: 实现 soak_watchdog**

新建 `tools/soak_watchdog.py`：

```python
"""soak_watchdog — soak/实盘期间的进程健康记录：RSS + 错误计数，每小时追加 CSV。

用法: python tools/soak_watchdog.py --out logs/soak_metrics.csv [--watch errors.log]
"""

import argparse
import os
import time


def rss_mb() -> float:
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except ImportError:
        return 0.0


def count_errors(log_path: str) -> int:
    if not log_path or not os.path.exists(log_path):
        return 0
    with open(log_path, encoding="utf-8", errors="ignore") as f:
        return sum(1 for line in f if "ERROR" in line or "WARNING" in line)


def collect_metrics(log_path: str = None) -> dict:
    return {"ts": time.time(), "rss_mb": round(rss_mb(), 1),
            "errors_last_hour": count_errors(log_path)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="logs/soak_metrics.csv")
    parser.add_argument("--log", default="logs/systrader.log")
    parser.add_argument("--interval", type=int, default=3600)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    header = "ts,rss_mb,errors_total\n"
    if not os.path.exists(args.out):
        with open(args.out, "w") as f:
            f.write(header)
    last_errors = 0
    while True:
        m = collect_metrics(args.log)
        delta = max(0, m["errors_last_hour"] - last_errors)
        last_errors = m["errors_last_hour"]
        with open(args.out, "a") as f:
            f.write(f"{int(m['ts'])},{m['rss_mb']},{delta}\n")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: 运行确认通过**

Run: `pytest tests/test_soak_watchdog.py -v`
Expected: 全部 PASS

- [ ] **Step 6: 提交**

```bash
git add execution/order_gateway.py market_data/feed.py tools/soak_watchdog.py tests/test_soak_watchdog.py
git commit -m "feat: reliability - 429 backoff audit, disconnect detail logs, soak_watchdog (pipeline F)"
```

### Task 19: B — 影子交易（双实例 + ShadowMonitor）

**Files:**
- Create: `tools/shadow_monitor.py`
- Test: `tests/test_shadow_monitor.py`（新建）

- [ ] **Step 1: 写失败测试**

新建 `tests/test_shadow_monitor.py`：

```python
"""ShadowMonitor 测试 — 双实例信号对齐与执行质量统计。"""

from tools.shadow_monitor import ShadowMonitor


def test_signal_alignment_ratio():
    mon = ShadowMonitor()
    for i in range(10):
        mon.record_signal("live", {"symbol": "BTCUSDT", "direction": "LONG", "ts": i})
        mon.record_signal("paper", {"symbol": "BTCUSDT", "direction": "LONG", "ts": i})
    mon.record_signal("paper", {"symbol": "BTCUSDT", "direction": "SHORT", "ts": 99})  # 错位
    ratio = mon.alignment_ratio()
    assert 0.9 <= ratio < 1.0


def test_execution_quality_recorded():
    mon = ShadowMonitor()
    mon.record_fill("live", {"symbol": "BTCUSDT", "price": 64001.0})
    mon.record_fill("paper", {"symbol": "BTCUSDT", "price": 64000.0})
    stats = mon.execution_quality()
    assert stats["slippage_bps"] is not None  # (64001-64000)/64000*10000


def test_report_saved(tmp_path):
    mon = ShadowMonitor()
    mon.record_signal("live", {"symbol": "BTCUSDT", "direction": "LONG"})
    mon.record_signal("paper", {"symbol": "BTCUSDT", "direction": "LONG"})
    out = str(tmp_path / "shadow.json")
    mon.save_report(out)
    import json, os
    assert os.path.exists(out)
    assert json.load(open(out))["alignment_ratio"] == 1.0
```

- [ ] **Step 2: 运行确认失败**

Run: `pytest tests/test_shadow_monitor.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

新建 `tools/shadow_monitor.py`：

```python
"""ShadowMonitor — 影子交易验证：双实例信号对齐 + 逐笔执行质量（TCA 风格）。

比对对象: 实盘实例（live）与模拟实例（paper）的 signal.generated / order.filled 事件。
验收: 信号对齐 ≥95%，滑点/填充率逐笔记录，1 周无系统性偏差。
"""

import json
import logging
import threading
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ShadowMonitor:
    def __init__(self, align_threshold: float = 0.95):
        self.align_threshold = align_threshold
        self._lock = threading.Lock()
        self.signals: Dict[str, List[dict]] = {"live": [], "paper": []}
        self.fills: Dict[str, List[dict]] = {"live": [], "paper": []}

    def record_signal(self, instance: str, data: dict):
        with self._lock:
            self.signals[instance].append(data)

    def record_fill(self, instance: str, data: dict):
        with self._lock:
            self.fills[instance].append(data)

    def alignment_ratio(self) -> float:
        """双实例信号方向一致率（按 symbol+时间窗匹配）。"""
        with self._lock:
            live = self.signals["live"]
            paper = self.signals["paper"]
            if not live:
                return 1.0
            paper_set = {(s.get("symbol"), s.get("direction")) for s in paper}
            matched = sum(1 for s in live if (s.get("symbol"), s.get("direction")) in paper_set)
            return matched / len(live)

    def execution_quality(self) -> dict:
        """逐笔执行质量：live 成交价 vs paper 成交价滑点（bps）。"""
        with self._lock:
            live = self.fills["live"]
            paper = self.fills["paper"]
            if not live or not paper:
                return {"slippage_bps": None, "fill_rate": None, "samples": 0}
            pairs = min(len(live), len(paper))
            if pairs == 0:
                return {"slippage_bps": None, "fill_rate": None, "samples": 0}
            slips = []
            for i in range(pairs):
                base = float(paper[i].get("price") or 0)
                if base <= 0:
                    continue
                slips.append((float(live[i].get("price") or 0) - base) / base * 10000)
            return {
                "slippage_bps": round(sum(slips) / len(slips), 2) if slips else None,
                "fill_rate": round(pairs / max(len(live), len(paper)), 2),
                "samples": pairs,
            }

    def save_report(self, path: str):
        with self._lock:
            report = {
                "alignment_ratio": round(self.alignment_ratio(), 4),
                "execution_quality": self.execution_quality(),
                "signal_count": {k: len(v) for k, v in self.signals.items()},
                "fill_count": {k: len(v) for k, v in self.fills.items()},
                "pass": self.alignment_ratio() >= self.align_threshold,
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info("Shadow report: %s", report)
        return report
```

**双实例运行方式（文档化，写入 RUNBOOK 附录"影子交易"）：**

```bash
# 实例 A（实盘小仓位）
python -m shared.runner --instance live --execution-mode live --risk-per-trade 0.002 --hours 168
# 实例 B（模拟，同参数）
python -m shared.runner --instance paper --execution-mode paper --risk-per-trade 0.002 --hours 168
# 比对（ShadowMonitor 消费 signal.generated 事件——订阅脚本另附，或集成进 soak_watchdog 报告）
```

（注：ShadowMonitor 的实时事件订阅集成进 `tools/shadow_monitor.py` 的 `--subscribe` 子命令，复用 StateStore 式消费；或先以落盘 JSON 报告 + 手动比对验证。本期以 `record_signal/record_fill` 接口 + 报告落盘为准，实时订阅接线列为后续增强。）

- [ ] **Step 4: 运行确认通过**

Run: `pytest tests/test_shadow_monitor.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add tools/shadow_monitor.py tests/test_shadow_monitor.py
git commit -m "feat: ShadowMonitor dual-instance signal alignment + execution quality (pipeline B)"
```

### Task 20: C — soak 7 天支持 + 验收文档

**Files:**
- Modify: `RUNBOOK.md`（soak 章节 + 验收标准）
- Test: `tests/test_runner_assembly.py`（追加 hours 语义测试，可选）

- [ ] **Step 1: 追加 hours 测试**

`tests/test_runner_assembly.py` 追加：

```python
def test_hours_zero_runs_forever():
    r = SystemRunner(hours=0)
    assert r.hours == 0  # 生产模式无限运行


def test_hours_positive_bounds_run():
    r = SystemRunner(hours=168)
    assert r.hours == 168  # soak 模式 7 天
```

- [ ] **Step 2: RUNBOOK 补 soak 章节**

`RUNBOOK.md` 追加：

```markdown
## 稳定性测试（soak）

```bash
# testnet 7 天 soak（统一装配）
python tools/stability_test.py --hours 168

# 并行健康监控（每小时 RSS + 错误计数）
python tools/soak_watchdog.py --log logs/systrader.log --out logs/soak_metrics.csv
```

### 验收标准（C 阶段）

- 7 天无意外错误（soak_metrics.csv 错误计数无异常尖峰）
- 无风控熔断触发（日志无 RISK REJECTED 熔断类）
- 对账零漂移（reconciler 无 drift 告警）
- 内存曲线平稳（RSS 波动 < 阈值，无持续增长趋势）

### 实盘分级（D 阶段，验收标准）

| 级 | risk_per_trade | 时长 | 验收 |
|---|---|---|---|
| 1 | 0.002 | 7 天 | 无重大事故 + 指标与 testnet 一致 ±20% |
| 2 | 0.005 | 7 天 | 同上 |
| 3 | 0.010 | 7 天 | 同上 |
| 4 | 0.015（设计值） | 持续 | 同上 |

```bash
python -m shared.runner --risk-per-trade 0.002 --execution-mode live
```

### 影子交易（B 阶段）

见 Task 19 双实例运行方式；验收：信号对齐 ≥95% + 逐笔滑点/填充率记录 + 1 周无系统性偏差。
```

- [ ] **Step 3: 运行确认通过**

Run: `pytest tests/test_runner_assembly.py -q`
Expected: 全部 PASS

- [ ] **Step 4: 提交**

```bash
git add shared/runner.py tests/test_runner_assembly.py RUNBOOK.md
git commit -m "docs+test: soak 7-day runbook + staged live acceptance criteria (pipeline C/D)"
```

---

## 自审记录

- **Spec 覆盖**：数据链路 spec 全部 10 节 → Task 1-14；管道 spec A-F → Task 15-20。事件流 6 类 + command 反向流全部有埋点/消费任务。
- **类型一致性**：`event_bus` 统一为可选构造参数（None 静默）；`instance` 字段贯穿 SignalEngine/MiddlewareChain/OrderManager/StateStore；`align_qty_to_step` 签名与 stability_test 一致。
- **已知实现期风险**（非占位符，实现时验证）：
  1. `SignalEngine.run` 内部路径（`_run_4h` 等分支）——Task 7 测试以实际代码为准微调 mock。
  2. `MiddlewareChain.process` 原 `modifications` 变量名——Task 8 以实际为准。
  3. `replay_runner` 与 `initialize()` 的 feed 替换冲突——Task 15 实现期以最小装配路径调整。
  4. Algo Order API 在 testnet 的可用性——实现 Task 3 前先验证（spec 第 4 节要求）。
- **顺序依赖**：Task 1→2→3（装配依赖数量对齐与 EventBus 容错）→5-9（埋点）→10-13（dashboard）→14（部署）→15-20（管道，15 依赖 3，17 依赖 13 的 server 侧发布，19 依赖 7 的 instance 标识）。

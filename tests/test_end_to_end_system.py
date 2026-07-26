"""
端到端系统测试 — 验证整个交易系统从头到脚是否能正常运作。

测试覆盖：
  Scenario 1: 完整交易链路（信号→风控→Paper执行→持仓→数据库）
  Scenario 2: 风控拒绝场景
  Scenario 3: Guardian 跟踪止损
  Scenario 4: 持久化写入与读取
  Scenario 5: 多标的并行处理
  Scenario 6: 异常恢复（价格中断、网络错误）
"""

import os, sys, time, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from signal_engine.engine import SignalEngine, Signal
from risk.chain import MiddlewareChain
from risk.position_sizer import PositionSizer
from risk.drawdown_breaker import DrawdownBreaker
from risk.daily_loss_limit import DailyLossLimit
from risk.concentration import ConcentrationCheck
from execution.order_manager import OrderManager, OrderState
from execution.order_gateway import OrderGateway, OrderRequest
from portfolio.tracker import PortfolioTracker, Position
from guardian.guardian import PositionGuardian, GuardianConfig, PositionState
from shared.database import TradeDatabase, TradeRecord
from shared.paper_trader import PaperTrader, PaperFill
from shared.config_loader import load_env


# ─── Fixtures ───

@pytest.fixture
def mock_feed():
    """可控制价格的模拟行情源。"""
    from unittest.mock import MagicMock
    feed = MagicMock()
    feed.get_last_price.return_value = 64000.0
    feed.get_mark_price.return_value = 64000.0
    feed.buffer.get_klines.return_value = []
    return feed


@pytest.fixture
def portfolio():
    return PortfolioTracker(initial_equity=10000.0)


@pytest.fixture
def risk_chain():
    chain = MiddlewareChain()
    chain.add(PositionSizer(risk_per_trade=0.015))
    chain.add(DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120))
    chain.add(DailyLossLimit(daily_loss_limit=0.05))
    chain.add(ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80))
    return chain


@pytest.fixture
def signal():
    return Signal(
        symbol="BTCUSDT", direction="LONG", conviction=0.72,
        entry_price=64000.0, stop_loss=63000.0, take_profit=66000.0,
    )


@pytest.fixture
def db():
    return TradeDatabase(":memory:")


# ═══════════════════════════════════════════════
# Scenario 1: 完整交易链路
# ═══════════════════════════════════════════════

class TestFullTradingPipeline:
    """模拟一次完整交易：信号→风控→下单→持仓→数据库"""

    def test_signal_to_position(self, mock_feed, portfolio, risk_chain, signal, db):
        # 1. Signal Engine 生成信号
        assert isinstance(signal, Signal)
        assert signal.direction == "LONG"
        assert signal.symbol == "BTCUSDT"

        # 2. 记录信号到数据库
        db.store_signal(signal.symbol, signal.direction, signal.conviction, signal.entry_price)
        signals = db.get_signals(limit=1)
        assert len(signals) == 1
        assert signals[0]["direction"] == "LONG"

        # 3. 风控检查
        result = risk_chain.process(signal, portfolio)
        assert not result.rejected, f"风控拒绝: {result.reason}"
        size = result.modifications.get("position_size", 0)
        assert size > 0, "仓位计算应为正数"

        # 4. Paper trading 模拟下单
        gateway = OrderGateway(testnet=True)
        trader = PaperTrader(feed=mock_feed, fill_delay_ms=0, slippage_pct=0.0)
        req = OrderRequest(symbol=signal.symbol, side="BUY", order_type="MARKET", quantity=0.001)
        fill = trader.execute(req)
        assert fill.status == "FILLED"
        assert fill.quantity == 0.001

        # 5. 记录成交到数据库
        trade = TradeRecord(
            symbol=fill.symbol, side=fill.side, order_type="MARKET",
            quantity=fill.quantity, price=fill.price, status=fill.status,
        )
        db.store_trade(trade)
        trades = db.get_trades()
        assert len(trades) == 1
        assert trades[0].symbol == "BTCUSDT"

        # 6. 更新持仓
        pos = Position(symbol=signal.symbol, direction=signal.direction,
                       quantity=fill.quantity, entry_price=fill.price, leverage=3)
        portfolio.open_position(pos)
        assert signal.symbol in portfolio.positions

        # 7. 验证持仓估值
        mark = mock_feed.get_mark_price(signal.symbol)
        upnl = portfolio.unrealized_pnl(signal.symbol, mark)
        assert isinstance(upnl, float)


# ═══════════════════════════════════════════════
# Scenario 2: 风控拒绝
# ═══════════════════════════════════════════════

class TestRiskRejections:
    """风控应在各种条件下正确拒绝交易"""

    def test_drawdown_breaker_rejects(self, portfolio, risk_chain, signal):
        portfolio.update_equity(10000.0)
        portfolio.peak_equity = 12000.0  # 回撤 16.7% > 15%
        result = risk_chain.process(signal, portfolio)
        assert result.rejected
        assert "DrawdownBreaker" in result.reason

    def test_daily_loss_limit_rejects(self, portfolio, risk_chain, signal):
        portfolio.daily_realized_pnl = -600.0  # 日亏损 6% > 5%
        result = risk_chain.process(signal, portfolio)
        assert result.rejected
        assert "DailyLossLimit" in result.reason

    def test_concentration_rejects(self, portfolio, risk_chain):
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.48, 62500.0, 3))
        sig = Signal("BTCUSDT", "LONG", 0.80, 62500.0, 61500.0, 65000.0)
        result = risk_chain.process(sig, portfolio)
        assert result.rejected
        assert "Concentration" in result.reason

    def test_chain_stops_at_first_rejection(self, portfolio, risk_chain, signal):
        portfolio.daily_realized_pnl = -600.0
        result = risk_chain.process(signal, portfolio)
        assert result.rejected
        # 应该在 DailyLossLimit 就停，不会继续到 PositionSizer
        assert "DailyLossLimit" in result.reason


# ═══════════════════════════════════════════════
# Scenario 3: Guardian 跟踪止损
# ═══════════════════════════════════════════════

class TestGuardianInPipeline:
    """持仓守护者与风控协同工作"""

    def test_trailing_stop_moves_with_price(self, mock_feed, portfolio):
        from unittest.mock import MagicMock
        gateway = MagicMock()
        gateway.place_order.return_value.status = "FILLED"
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        guardian = PositionGuardian(feed=mock_feed, portfolio=portfolio, gateway=gateway, config=None)
        guardian._position_state["BTCUSDT"] = PositionState(
            "BTCUSDT", "LONG", 60000.0, 60000.0, 58000.0,
        )
        # 价格涨到 62000
        mock_feed.get_last_price.return_value = 62000.0
        guardian._check_positions()
        new_stop = guardian._position_state["BTCUSDT"].current_stop
        assert new_stop > 58000.0, "跟踪止损应上移"

    def test_guardian_no_action_without_positions(self):
        """无持仓时 Guardian 不报错"""
        empty_portfolio = PortfolioTracker()
        from unittest.mock import MagicMock
        guardian = PositionGuardian(feed=MagicMock(), portfolio=empty_portfolio, gateway=MagicMock())
        guardian._check_positions()  # 不应抛异常

    def test_tp1_partial_close_reduces_remaining(self, mock_feed, portfolio):
        """TP1 后 closed_qty 增加，避免超卖"""
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        from unittest.mock import MagicMock
        gateway = MagicMock()
        gateway.place_order.return_value.status = "FILLED"

        guardian = PositionGuardian(feed=mock_feed, portfolio=portfolio, gateway=gateway)
        guardian._position_state["BTCUSDT"] = PositionState(
            "BTCUSDT", "LONG", 60000.0, 60000.0, 58000.0,
        )
        mock_feed.get_last_price.return_value = 62000.0
        guardian._check_tp(guardian._position_state["BTCUSDT"], 62000.0)
        assert guardian._position_state["BTCUSDT"].tp1_done
        assert guardian._position_state["BTCUSDT"].closed_qty > 0


# ═══════════════════════════════════════════════
# Scenario 4: 持久化
# ═══════════════════════════════════════════════

class TestPersistence:
    """数据库读写验证"""

    def test_trade_roundtrip(self, db):
        trade = TradeRecord(symbol="ETHUSDT", side="SELL", order_type="LIMIT",
                            quantity=0.5, price=3100.0, status="FILLED", order_id=999)
        db.store_trade(trade)
        trades = db.get_trades()
        assert len(trades) == 1
        assert trades[0].symbol == "ETHUSDT"
        assert trades[0].price == 3100.0

    def test_multiple_trades_ordered(self, db):
        for i in range(5):
            db.store_trade(TradeRecord(symbol="BTCUSDT", side="BUY", order_type="MARKET",
                                        quantity=0.01 * (i + 1), price=64000.0, status="FILLED"))
        trades = db.get_trades(limit=3)
        assert len(trades) == 3  # 最新的 3 条

    def test_signal_with_metadata(self, db):
        db.store_signal("SOLUSDT", "SHORT", 0.65, 150.0, {"strategy": "momentum"})
        signals = db.get_signals()
        meta = json.loads(signals[0]["metadata"])
        assert meta["strategy"] == "momentum"


# ═══════════════════════════════════════════════
# Scenario 5: 多标的并行处理
# ═══════════════════════════════════════════════

class TestMultiSymbol:
    """多个标的同时运行时互不干扰"""

    def test_multiple_positions_independent(self, mock_feed, portfolio, risk_chain):
        # 开两个仓
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        portfolio.open_position(Position("ETHUSDT", "SHORT", 1.0, 3000.0, 3))
        assert len(portfolio.positions) == 2

        # 各自计算未实现盈亏
        btc_pnl = portfolio.unrealized_pnl("BTCUSDT", 64000.0)
        eth_pnl = portfolio.unrealized_pnl("ETHUSDT", 2900.0)
        assert btc_pnl > 0  # BTC 涨了
        assert eth_pnl > 0  # ETH 跌了，做空赚钱

    def test_risk_checks_each_symbol_independently(self, portfolio, risk_chain):
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.15, 62500.0, 3))
        # ETH 信号不应被 BTC 仓位拒绝
        eth_signal = Signal("ETHUSDT", "LONG", 0.68, 3100.0, 3000.0, 3400.0)
        result = risk_chain.process(eth_signal, portfolio)
        assert not result.rejected

    def test_concentration_blocks_same_symbol(self, portfolio, risk_chain):
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.48, 62500.0, 3))
        signal = Signal("BTCUSDT", "LONG", 0.80, 62500.0, 61500.0, 65000.0)
        result = risk_chain.process(signal, portfolio)
        assert result.rejected
        assert "Concentration" in result.reason


# ═══════════════════════════════════════════════
# Scenario 6: 异常恢复
# ═══════════════════════════════════════════════

class TestErrorRecovery:
    """价格中断、网络错误等异常情况"""

    def test_price_none_does_not_crash(self, portfolio):
        """价格中断时系统不崩溃"""
        from unittest.mock import MagicMock
        feed = MagicMock()
        feed.get_last_price.return_value = None
        feed.get_mark_price.return_value = None
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        guardian = PositionGuardian(feed=feed, portfolio=portfolio, gateway=None)
        guardian._check_positions()  # 不应抛异常

    def test_signal_with_zero_conviction(self, risk_chain, portfolio):
        """信念值=0的信号"""
        signal = Signal("BTCUSDT", "LONG", 0.0, 64000.0, 63000.0, 66000.0)
        result = risk_chain.process(signal, portfolio)
        assert not result.rejected  # 风控不检查信念值

    def test_portfolio_close_updates_state(self, portfolio):
        """平仓后状态更新正确"""
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        pnl = portfolio.close_position("BTCUSDT", 62000.0)
        assert pnl > 0
        assert portfolio.positions == {}
        assert portfolio.consecutive_losses == 0

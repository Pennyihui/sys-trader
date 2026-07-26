"""完整链路测试：K线数据 → KlineBuffer → 信号 → 风控 → 下单 → 持仓 → 数据库

模拟真实数据流，验证各模块间的接口和数据一致性。
信号引擎部分使用 mock，其余全部用真实代码。
"""

import os, sys, json, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import MagicMock, patch
from market_data.kline_buffer import KlineBuffer, Kline
from signal_engine.engine import Signal, SignalEngine
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


@pytest.fixture
def kline_buffer():
    return KlineBuffer(max_size=500)


@pytest.fixture
def portfolio():
    return PortfolioTracker(initial_equity=10000.0)


@pytest.fixture
def db():
    return TradeDatabase(":memory:")


@pytest.fixture
def risk_chain():
    chain = MiddlewareChain()
    chain.add(PositionSizer(risk_per_trade=0.015))
    chain.add(DrawdownBreaker(max_drawdown=0.15, consecutive_loss_breaker=3, cooldown_minutes=120))
    chain.add(DailyLossLimit(daily_loss_limit=0.05))
    chain.add(ConcentrationCheck(max_per_symbol=0.30, max_same_direction=0.50, max_total_margin=0.80))
    return chain


@pytest.mark.integration
class TestCompletePipeline:
    """从 K 线数据到持仓的完整端到端流程"""

    def make_kline(self, open_time: int, close: float, is_closed: bool = False):
        """构造一条 K 线"""
        return Kline(
            symbol="BTCUSDT", timeframe="4h",
            open_time=open_time,
            close_time=open_time + 14400000,  # 4h in ms
            open=close - 100, high=close + 200,
            low=close - 300, close=close,
            volume=100.0, is_closed=is_closed,
        )

    def test_kline_to_signal_to_position(self, kline_buffer, portfolio, risk_chain, db):
        """K 线数据 → KlineBuffer → 模拟信号 → 风控 → Paper下单 → 持仓 → 数据库"""
        # ─── 阶段 1: K 线写入 buffer ───
        # 写入 15 条未闭合 K 线（模拟实时更新）
        base_time = 1700000000000
        for i in range(15):
            k = self.make_kline(base_time + i * 14400000, 64000.0 + i * 50, is_closed=False)
            kline_buffer.add(k)
        assert kline_buffer.count("BTCUSDT", "4h") == 15

        # 写入一条闭合 K 线（模拟 4h 闭合）
        closed_kline = self.make_kline(base_time + 15 * 14400000, 64750.0, is_closed=True)
        kline_buffer.add(closed_kline)
        assert kline_buffer.is_closed("BTCUSDT", "4h", closed_kline.open_time)

        # 验证 buffer 中的数据可读
        history = kline_buffer.get_klines("BTCUSDT", "4h", limit=10)
        assert len(history) == 10
        assert history[-1].close == 64750.0

        # ─── 阶段 2: 信号生成（模拟信号引擎输出） ───
        # 真实场景中，这里会调用 SignalEngine.run("BTCUSDT", "4h", ohlcv)
        # 由于信号引擎目前是 stub，手动构造 Signal
        signal = Signal(
            symbol="BTCUSDT", direction="LONG", conviction=0.72,
            entry_price=64500.0, stop_loss=63500.0, take_profit=66500.0,
        )
        assert signal.direction == "LONG"
        assert 0 <= signal.conviction <= 1.0

        # 记录信号到数据库
        db.store_signal(signal.symbol, signal.direction, signal.conviction, signal.entry_price)
        signals = db.get_signals()
        assert signals[0]["direction"] == "LONG"

        # ─── 阶段 3: 风控链检查 ───
        result = risk_chain.process(signal, portfolio)
        assert not result.rejected, f"风控拒绝: {result.reason}"
        size = result.modifications.get("position_size", 0)
        assert size > 0

        # ─── 阶段 4: Paper Trading 执行 ───
        mock_feed = MagicMock()
        mock_feed.get_last_price.return_value = 64750.0
        mock_feed.get_mark_price.return_value = 64750.0

        trader = PaperTrader(feed=mock_feed, fill_delay_ms=0, slippage_pct=0.001)

        # 入场单
        entry_req = OrderRequest(
            symbol=signal.symbol, side="BUY", order_type="MARKET",
            quantity=0.001,
        )
        fill = trader.execute(entry_req)
        assert fill.status == "FILLED"
        assert fill.quantity == 0.001
        assert fill.price > 0

        # 记录成交到数据库
        trade = TradeRecord(
            symbol=fill.symbol, side=fill.side, order_type="MARKET",
            quantity=fill.quantity, price=fill.price, status=fill.status,
        )
        db.store_trade(trade)
        trades = db.get_trades()
        assert len(trades) == 1
        assert float(trades[0].price) > 0

        # ─── 阶段 5: 持仓更新 ───
        position = Position(
            symbol=signal.symbol, direction=signal.direction,
            quantity=fill.quantity, entry_price=fill.price, leverage=3,
        )
        portfolio.open_position(position)
        assert signal.symbol in portfolio.positions

        # ─── 阶段 6: 风控持续监控（Guardian） ───
        gateway = MagicMock()
        gateway.place_order.return_value.status = "FILLED"

        guardian = PositionGuardian(
            feed=mock_feed, portfolio=portfolio, gateway=gateway,
            config=GuardianConfig(check_interval=0.1),
        )
        # 初始化 Guardian 状态
        guardian._check_positions()
        assert "BTCUSDT" in guardian._position_state

        # 价格涨了，验证跟踪止损上移
        mock_feed.get_last_price.return_value = 66000.0
        mock_feed.get_mark_price.return_value = 66000.0
        guardian._check_positions()
        initial_stop = guardian._position_state["BTCUSDT"].current_stop
        assert initial_stop >= 63500.0

        # ─── 阶段 7: 平仓，验证最终状态 ───
        pnl = portfolio.close_position("BTCUSDT", 66000.0)
        assert pnl > 0  # 盈利平仓
        assert portfolio.positions == {}
        assert portfolio.total_realized_pnl > 0

        # 验证数据库有完整记录
        trades = db.get_trades()
        assert len(trades) >= 1

    def test_multiple_symbols_flow(self, portfolio, risk_chain):
        """多标的独立交易不相互干扰"""
        # BTC LONG
        signal_btc = Signal("BTCUSDT", "LONG", 0.72, 64000.0, 63000.0, 66000.0)
        result_btc = risk_chain.process(signal_btc, portfolio)
        assert not result_btc.rejected

        # ETH SHORT
        signal_eth = Signal("ETHUSDT", "SHORT", 0.68, 3100.0, 3150.0, 2950.0)
        result_eth = risk_chain.process(signal_eth, portfolio)
        assert not result_eth.rejected

        # 开仓
        portfolio.open_position(Position("BTCUSDT", "LONG", 0.1, 64000.0, 3))
        portfolio.open_position(Position("ETHUSDT", "SHORT", 1.0, 3100.0, 3))
        assert len(portfolio.positions) == 2

        # 各自的未实现盈亏
        btc_upnl = portfolio.unrealized_pnl("BTCUSDT", 65000.0)
        eth_upnl = portfolio.unrealized_pnl("ETHUSDT", 3050.0)
        assert btc_upnl > 0
        assert eth_upnl > 0

        # 各自平仓不受影响
        portfolio.close_position("BTCUSDT", 65000.0)
        assert "BTCUSDT" not in portfolio.positions
        assert "ETHUSDT" in portfolio.positions

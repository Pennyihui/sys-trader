"""边界情况测试 — Guardian, OrderManager, Risk 边缘场景。"""

import pytest
from unittest.mock import MagicMock
from guardian.guardian import PositionGuardian, GuardianConfig, PositionState
from portfolio.tracker import PortfolioTracker, Position
from risk.chain import MiddlewareChain
from risk.position_sizer import PositionSizer
from risk.daily_loss_limit import DailyLossLimit
from signal_engine.engine import Signal


@pytest.mark.integration
class TestGuardianEdgeCases:
    def setup_method(self):
        self.feed = MagicMock()
        self.feed.get_last_price.return_value = None  # 价格丢失
        self.feed.get_mark_price.return_value = None
        self.tracker = PortfolioTracker(initial_equity=10000.0)
        self.gateway = MagicMock()
        self.guardian = PositionGuardian(
            feed=self.feed, portfolio=self.tracker, gateway=self.gateway,
        )

    def test_price_none_does_not_crash(self):
        """get_last_price 返回 None 时跳过，不崩溃"""
        pos = Position(symbol="BTCUSDT", direction="LONG",
                       quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        self.guardian._check_positions()  # 不应抛异常

    def test_multiple_positions_independent(self):
        """多个持仓互不影响"""
        self.feed.get_last_price.return_value = 62000.0
        self.tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        self.tracker.open_position(Position("ETHUSDT", "SHORT", 1.0, 3000.0, 3))
        self.guardian._check_positions()
        assert "BTCUSDT" in self.guardian._position_state
        assert "ETHUSDT" in self.guardian._position_state

    def test_partial_close_then_tp2(self):
        """TP1 之后 TP2 应该用剩余数量"""
        self.feed.get_last_price.return_value = 64000.0
        pos = Position("BTCUSDT", "LONG", 0.1, 60000.0, 3)
        self.tracker.open_position(pos)
        self.guardian._position_state["BTCUSDT"] = PositionState(
            "BTCUSDT", "LONG", 60000.0, 60000.0, 58000.0,
            tp1_done=True, closed_qty=0.05,
        )
        self.guardian._check_tp(self.guardian._position_state["BTCUSDT"], 64000.0)
        # 检查 TP2 的 quantity 不应超过剩余 (0.1 - 0.05 = 0.05)
        calls = self.gateway.place_order.call_args_list
        if calls:
            qty = calls[-1][0][0].quantity
            assert qty <= 0.05, f"TP2 超卖: {qty} > 0.05"


@pytest.mark.integration
class TestRiskEdgeCases:
    def test_zero_equity_rejected(self):
        """权益为 0 时拒绝所有交易"""
        tracker = PortfolioTracker(initial_equity=0.0)
        limit = DailyLossLimit(daily_loss_limit=0.05)
        signal = Signal("BTCUSDT", "LONG", 0.72, 62500.0, 61500.0, 65000.0)
        result = limit.process(signal, tracker)
        assert result.rejected

    def test_sizer_with_tiny_account(self):
        """小账户仓位计算不报错"""
        tracker = PortfolioTracker(initial_equity=100.0)
        sizer = PositionSizer(risk_per_trade=0.1)
        signal = Signal("BTCUSDT", "LONG", 0.72, 62500.0, 61000.0, 64000.0)
        result = sizer.process(signal, tracker)
        assert not result.rejected

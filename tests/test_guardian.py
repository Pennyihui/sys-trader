"""测试 PositionGuardian 核心逻辑"""
import pytest
from unittest.mock import MagicMock, PropertyMock
from guardian.guardian import PositionGuardian, GuardianConfig, PositionState
from portfolio.tracker import PortfolioTracker, Position


@pytest.mark.integration
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
        self.guardian._check_positions()
        self.gateway.place_order.assert_not_called()
        self.gateway.place_algo_order.assert_not_called()

    def test_trailing_stop_moves_up(self):
        """价格上涨时止损应上移"""
        pos = Position(symbol="BTCUSDT", direction="LONG",
                       quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        self.guardian._position_state["BTCUSDT"] = PositionState(
            symbol="BTCUSDT", direction="LONG", entry_price=60000.0,
            highest_price=60000.0, current_stop=58000.0,
        )
        # 价格涨到 62000
        self.feed.get_last_price.return_value = 62000.0
        self.guardian._check_positions()
        new_stop = self.guardian._position_state["BTCUSDT"].current_stop
        assert new_stop > 58000.0

    def test_dynamic_stop_atr(self):
        """ATR 缓存生效"""
        import time
        self.guardian._atr_cache["BTCUSDT"] = 2000.0
        self.guardian._atr_last_update["BTCUSDT"] = time.time()
        assert self.guardian._ensure_atr("BTCUSDT") == 2000.0

    def test_tp1_partial_close(self):
        """达到 TP1 时发 MARKET 单平 50% (tp1_ratio 生效), 非全平"""
        self.gateway.place_order.return_value = MagicMock()
        self.gateway.place_order.return_value.status = "FILLED"
        self.gateway.place_order.return_value.avg_price = 62000.0

        pos = Position(symbol="BTCUSDT", direction="LONG",
                       quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        self.guardian._position_state["BTCUSDT"] = PositionState(
            symbol="BTCUSDT", direction="LONG", entry_price=60000.0,
            highest_price=60000.0, current_stop=58000.0,
        )
        self.feed.get_last_price.return_value = 62000.0
        self.guardian._check_positions()
        assert self.guardian._position_state["BTCUSDT"].tp1_done is True
        # 只平 50%: 下单价量 = 0.1 * 0.5 = 0.05
        req = self.gateway.place_order.call_args[0][0]
        assert req.quantity == pytest.approx(0.05, abs=0.0001)
        # 持仓数量同步减半, 未全平
        assert self.tracker.positions["BTCUSDT"].quantity == pytest.approx(0.05, abs=0.0001)
        assert self.tracker.positions["BTCUSDT"] is pos

    def test_tp1_then_tp2_closes_remaining(self):
        """TP1 平半后价格继续走高, TP2 平剩余全部并移除持仓"""
        self.gateway.place_order.return_value = MagicMock()
        self.gateway.place_order.return_value.status = "FILLED"
        self.gateway.place_order.return_value.avg_price = 68000.0

        pos = Position(symbol="BTCUSDT", direction="LONG",
                       quantity=0.1, entry_price=60000.0, leverage=3)
        self.tracker.open_position(pos)
        self.guardian._position_state["BTCUSDT"] = PositionState(
            symbol="BTCUSDT", direction="LONG", entry_price=60000.0,
            highest_price=60000.0, current_stop=58000.0,
        )
        # 直接触发 TP2 路径 (pnl 13.3% > tp2_pct 6%): TP1 先平半, TP2 平剩余
        self.feed.get_last_price.return_value = 68000.0
        self.guardian._check_tp(self.guardian._position_state["BTCUSDT"], 68000.0)
        state = self.guardian._position_state["BTCUSDT"]
        assert state.tp1_done and state.tp2_done
        assert "BTCUSDT" not in self.tracker.positions

    def test_init_atr_default_when_no_kline(self):
        """没K线数据时 ATR 默认值 500"""
        self.feed.buffer.get_klines.return_value = []
        atr = self.guardian._calc_atr("BTCUSDT")
        assert atr == 500.0

    def test_cleanup_removed_positions(self):
        """已平仓的持仓应自动清理"""
        self.guardian._position_state["BTCUSDT"] = PositionState(
            symbol="BTCUSDT", direction="LONG", entry_price=60000.0,
            highest_price=60000.0, current_stop=58000.0,
        )
        self.guardian._check_positions()
        assert "BTCUSDT" not in self.guardian._position_state

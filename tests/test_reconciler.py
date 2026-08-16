import pytest
from unittest.mock import MagicMock
from shared.reconciler import PositionReconciler
from portfolio.tracker import PortfolioTracker, Position


class TestReconciler:
    def test_no_drift_when_consistent(self):
        gw = MagicMock()
        gw.get_account.return_value = {
            "positions": [{"symbol": "BTCUSDT", "positionAmt": "0.1"}],
        }
        tracker = PortfolioTracker()
        tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        rec = PositionReconciler(gw, tracker)
        report = rec.reconcile()
        assert report.drift is False

    def test_drift_remote_only(self):
        gw = MagicMock()
        gw.get_account.return_value = {
            "positions": [{"symbol": "BTCUSDT", "positionAmt": "0.1"}],
        }
        tracker = PortfolioTracker()
        rec = PositionReconciler(gw, tracker)
        report = rec.reconcile()
        assert report.drift is True
        assert "remote_only" in report.details

    def test_drift_local_only(self):
        gw = MagicMock()
        gw.get_account.return_value = {"positions": []}
        tracker = PortfolioTracker()
        tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        rec = PositionReconciler(gw, tracker)
        report = rec.reconcile()
        assert report.drift is True
        assert "local_only" in report.details
        assert "BTCUSDT" in report.details["local_only"]

    def test_drift_qty_mismatch(self):
        gw = MagicMock()
        gw.get_account.return_value = {
            "positions": [{"symbol": "BTCUSDT", "positionAmt": "0.2"}],
        }
        tracker = PortfolioTracker()
        tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        rec = PositionReconciler(gw, tracker)
        report = rec.reconcile()
        assert report.drift is True
        assert len(report.details["qty_mismatch"]) == 1
        m = report.details["qty_mismatch"][0]
        assert m["symbol"] == "BTCUSDT"
        assert m["local"] == 0.1
        assert m["remote"] == 0.2

    def test_on_drift_callback_invoked(self):
        gw = MagicMock()
        gw.get_account.return_value = {
            "positions": [{"symbol": "BTCUSDT", "positionAmt": "0.1"}],
        }
        tracker = PortfolioTracker()
        callback = MagicMock()
        rec = PositionReconciler(gw, tracker, on_drift=callback)
        report = rec.reconcile()
        callback.assert_called_once_with(report)

    def test_fetch_remote_error(self):
        """账户接口异常 → 跳过本轮对账, 不再误判 local_only (2026-08-16 修复)。"""
        gw = MagicMock()
        gw.get_account.side_effect = Exception("Network error")
        tracker = PortfolioTracker()
        tracker.open_position(Position("ETHUSDT", "SHORT", 1.0, 3200.0, 2))
        rec = PositionReconciler(gw, tracker)
        report = rec.reconcile()
        assert report.drift is False
        assert "ETHUSDT" in tracker.positions

    def test_account_error_response_skips_cycle(self):
        """账户响应含 error 字段 → 跳过本轮对账, 不产生 local_only 假漂移
        (2026-08-16: 代理抖动时曾把拉取失败误判为"持仓消失", 每 5 分钟假平仓)。"""
        gw = MagicMock()
        gw.get_account.return_value = {"error": "SSL EOF"}
        tracker = PortfolioTracker()
        tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        callback = MagicMock()
        rec = PositionReconciler(gw, tracker, on_drift=callback)
        report = rec.reconcile()
        assert report.drift is False
        callback.assert_not_called()
        assert "BTCUSDT" in tracker.positions  # 本地持仓保留

    def test_account_exception_skips_cycle(self):
        """账户接口抛异常 → 跳过本轮对账。"""
        gw = MagicMock()
        gw.get_account.side_effect = Exception("Connection reset")
        tracker = PortfolioTracker()
        tracker.open_position(Position("ETHUSDT", "SHORT", 1.0, 3200.0, 2))
        rec = PositionReconciler(gw, tracker)
        report = rec.reconcile()
        assert report.drift is False
        assert "ETHUSDT" in tracker.positions

    def test_missing_positions_key_skips_cycle(self):
        """响应缺 positions 字段 (异常结构) → 跳过, 不误判。"""
        gw = MagicMock()
        gw.get_account.return_value = {"totalWalletBalance": "1000"}
        tracker = PortfolioTracker()
        tracker.open_position(Position("BTCUSDT", "LONG", 0.1, 60000.0, 3))
        rec = PositionReconciler(gw, tracker)
        report = rec.reconcile()
        assert report.drift is False
        assert "BTCUSDT" in tracker.positions

    def test_start_stop(self):
        gw = MagicMock()
        gw.get_account.return_value = {"positions": []}
        tracker = PortfolioTracker()
        rec = PositionReconciler(gw, tracker, interval=99999)
        rec.start()
        assert rec._stop is not None
        assert rec._thread is not None and rec._thread.is_alive()
        rec.stop()
        assert rec._stop.is_set() is True

"""风控补强 + 运营财务 + 执行层/面板 测试 (2026-08-16 第六轮)。

覆盖:
  #1 保证金率自动减仓 (runner._protective_check/_protective_close)
  #2 回撤分级响应 (减仓档, 回落重新武装)
  #3 单日最大交易次数 (DailyTradeLimit)
  #4 最大止损距离 (MaxStopDistance)
  #5 每日钉钉运营摘要 (digest 合成 + 窗口去重)
  #6 精确资金费对账 (get_income + tranId 游标)
  #7 钉钉 @人 (send_at)
  #8 IOC 入场单选项
  #9 部分成交余量策略
  #10 盈亏平衡价 (DataCollector break_even)
"""

import json
import os
import time
from unittest.mock import MagicMock

import pytest

from execution.order_gateway import OrderGateway
from execution.order_manager import OrderManager, OrderState
from portfolio.tracker import PortfolioTracker, Position
from risk.daily_trade_limit import DailyTradeLimit
from risk.max_stop_distance import MaxStopDistance
from signal_engine.engine import Signal
from shared.execution_mode import ExecutionMode, ExecutionModeManager


def _signal(entry=64000.0, stop=63000.0, tp=66000.0):
    return Signal(symbol="BTCUSDT", direction="LONG", conviction=0.72,
                  entry_price=entry, stop_loss=stop, take_profit=tp)


def _live_om(gw):
    return OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode("live")))


# ─── #3 单日最大交易次数 ───

class TestDailyTradeLimit:
    def test_passes_below_limit(self):
        tracker = PortfolioTracker(initial_equity=1000.0)
        tracker.trade_count_today = 5
        res = DailyTradeLimit(max_trades=30).process(_signal(), tracker)
        assert not res.rejected

    def test_rejects_at_limit(self):
        tracker = PortfolioTracker(initial_equity=1000.0)
        tracker.trade_count_today = 30
        res = DailyTradeLimit(max_trades=30).process(_signal(), tracker)
        assert res.rejected
        assert "DailyTradeLimit" in res.reason

    def test_zero_disables(self):
        tracker = PortfolioTracker(initial_equity=1000.0)
        tracker.trade_count_today = 999
        res = DailyTradeLimit(max_trades=0).process(_signal(), tracker)
        assert not res.rejected

    def test_daily_reset(self):
        import datetime
        tracker = PortfolioTracker(initial_equity=1000.0)
        tracker.trade_count_today = 30  # 模拟当日已打满
        tracker._last_reset_day = datetime.date(2000, 1, 1)  # 强制日切
        # 日切重置在 open_position 内发生
        tracker.open_position(Position("BTCUSDT", "LONG", 0.001, 60000.0, 3))
        assert tracker.trade_count_today == 1
        res = DailyTradeLimit(max_trades=30).process(_signal(), tracker)
        assert not res.rejected


# ─── #4 最大止损距离 ───

class TestMaxStopDistance:
    def test_rejects_far_stop(self):
        sig = _signal(entry=100.0, stop=90.0, tp=110.0)  # 距离 10%
        res = MaxStopDistance(max_stop_pct=0.05).process(sig, PortfolioTracker(1000.0))
        assert res.rejected
        assert "MaxStopDistance" in res.reason

    def test_passes_within_limit(self):
        sig = _signal(entry=100.0, stop=97.0, tp=110.0)  # 距离 3%
        res = MaxStopDistance(max_stop_pct=0.05).process(sig, PortfolioTracker(1000.0))
        assert not res.rejected

    def test_zero_disables(self):
        sig = _signal(entry=100.0, stop=1.0, tp=200.0)
        res = MaxStopDistance(max_stop_pct=0).process(sig, PortfolioTracker(1000.0))
        assert not res.rejected


# ─── #7 钉钉 @人 ───

class TestDingTalkAt:
    def test_send_at_includes_mobiles_and_keyword(self, monkeypatch):
        from monitor.dingtalk import DingTalkNotifier
        n = DingTalkNotifier("https://example.com/hook")
        captured = {}
        monkeypatch.setattr(n, "_post", lambda payload: captured.update(payload) or True)
        assert n.send_at("熔断告警", ["13800000000"])
        assert captured["at"] == {"atMobiles": ["13800000000"], "isAtAll": False}
        assert "[SysTrader]" in captured["text"]["content"]

    def test_send_at_empty_mobiles_falls_back_to_send(self, monkeypatch):
        from monitor.dingtalk import DingTalkNotifier
        n = DingTalkNotifier("https://example.com/hook")
        captured = {}
        monkeypatch.setattr(n, "_post", lambda payload: captured.update(payload) or True)
        assert n.send_at("普通告警", [])
        assert "at" not in captured


# ─── #8 IOC 入场单 ───

def _new_order_gw():
    gw = MagicMock()
    gw.place_order.return_value = MagicMock(
        order_id=1, symbol="BTCUSDT", side="BUY", status="NEW",
        executed_qty=0.0, avg_price=0.0, error=None, code=None)
    return gw


class TestIocEntry:
    def test_ioc_request_carries_time_in_force(self, monkeypatch):
        monkeypatch.setenv("ENTRY_TIF", "IOC")
        monkeypatch.setenv("POST_ONLY", "0")
        gw = _new_order_gw()
        om = _live_om(gw)
        om.submit_entry("BTCUSDT", "LONG", 0.001, 64000.0, 63000.0, 66000.0)
        req = gw.place_order.call_args[0][0]
        assert req.time_in_force == "IOC"

    def test_ioc_disables_post_only(self, monkeypatch):
        monkeypatch.setenv("ENTRY_TIF", "IOC")
        monkeypatch.setenv("POST_ONLY", "1")
        gw = _new_order_gw()
        om = _live_om(gw)
        om.submit_entry("BTCUSDT", "LONG", 0.001, 64000.0, 63000.0, 66000.0)
        req = gw.place_order.call_args[0][0]
        assert req.post_only is False

    def test_gtc_default(self, monkeypatch):
        monkeypatch.delenv("ENTRY_TIF", raising=False)
        monkeypatch.setenv("POST_ONLY", "0")
        gw = _new_order_gw()
        om = _live_om(gw)
        om.submit_entry("BTCUSDT", "LONG", 0.001, 64000.0, 63000.0, 66000.0)
        req = gw.place_order.call_args[0][0]
        assert req.time_in_force == "GTC"


# ─── #9 部分成交余量策略 ───

def _partial_gateway():
    gw = MagicMock()
    place = MagicMock(order_id=7, symbol="BTCUSDT", side="BUY",
                      status="PARTIALLY_FILLED", executed_qty=0.0005,
                      avg_price=64000.0, error=None, code=None)
    gw.place_order.return_value = place
    cancel = MagicMock(order_id=7, symbol="BTCUSDT", side="BUY",
                       status="CANCELED", executed_qty=0.0005,
                       avg_price=64000.0, error=None)
    gw.cancel_order.return_value = cancel
    return gw


class TestPartialFillPolicy:
    def test_cancel_policy_cancels_remainder(self, monkeypatch):
        monkeypatch.setenv("PARTIAL_FILL_POLICY", "cancel")
        monkeypatch.setenv("POST_ONLY", "0")
        gw = _partial_gateway()
        om = _live_om(gw)
        order = om.submit_entry("BTCUSDT", "LONG", 0.001, 64000.0, 63000.0, 66000.0)
        assert order.state == OrderState.CANCELED
        assert order.remainder_canceled is True
        gw.cancel_order.assert_called_once()

    def test_wait_policy_keeps_remainder(self, monkeypatch):
        monkeypatch.setenv("PARTIAL_FILL_POLICY", "wait")
        monkeypatch.setenv("POST_ONLY", "0")
        gw = _partial_gateway()
        om = _live_om(gw)
        order = om.submit_entry("BTCUSDT", "LONG", 0.001, 64000.0, 63000.0, 66000.0)
        assert order.state == OrderState.PARTIALLY_FILLED
        gw.cancel_order.assert_not_called()

    def test_cancel_error_retries(self, monkeypatch):
        monkeypatch.setenv("PARTIAL_FILL_POLICY", "cancel")
        monkeypatch.setenv("POST_ONLY", "0")
        gw = _partial_gateway()
        gw.cancel_order.return_value = MagicMock(
            order_id=7, symbol="BTCUSDT", side="BUY", status="ERROR",
            executed_qty=0.0005, avg_price=64000.0, error="timeout")
        om = _live_om(gw)
        order = om.submit_entry("BTCUSDT", "LONG", 0.001, 64000.0, 63000.0, 66000.0)
        # 撤单网络失败: 保持部分成交 + 复位标记, 下轮重试
        assert order.state == OrderState.PARTIALLY_FILLED
        assert order.remainder_canceled is False


# ─── #6 精确资金费对账 ───

class _FakeGateway:
    def __init__(self, records):
        self.records = records

    def get_income(self, income_type="FUNDING_FEE", start_time_ms=None, limit=1000):
        return self.records


def _make_runner(tmp_path, records, last_tran_file=None, accounting="income"):
    from shared.runner import SystemRunner
    runner = SystemRunner.__new__(SystemRunner)  # 跳过 __init__ 的信号注册
    runner.execution_mode = MagicMock()
    runner.execution_mode.is_live.return_value = True
    runner.portfolio = PortfolioTracker(initial_equity=1000.0)
    runner.gateway = _FakeGateway(records)
    runner._funding_accounting = accounting
    runner._funding_state_path = str(tmp_path / "funding_state.json")
    runner._funding_last_tran = None
    if last_tran_file is not None:
        (tmp_path / "funding_state.json").write_text(
            json.dumps({"last_tran": last_tran_file}), encoding="utf-8")
    return runner


class TestFundingIncomeReconciler:
    def test_first_run_seeds_cursor_without_booking(self, tmp_path):
        records = [
            {"tranId": 101, "income": "-0.5", "asset": "USDT"},
            {"tranId": 100, "income": "-0.3", "asset": "USDT"},
        ]
        runner = _make_runner(tmp_path, records)
        runner._sync_funding_income()
        assert runner._funding_last_tran == 101
        assert runner.portfolio.total_funding_fees == 0.0  # 历史不补记
        saved = json.loads((tmp_path / "funding_state.json").read_text(encoding="utf-8"))
        assert saved["last_tran"] == 101

    def test_incremental_books_only_new_negative_income(self, tmp_path):
        records = [
            {"tranId": 101, "income": "-0.5", "asset": "USDT"},
            {"tranId": 100, "income": "-0.3", "asset": "USDT"},  # 游标内, 忽略
            {"tranId": 102, "income": "0.1", "asset": "USDT"},   # 返还, 不记账
            {"tranId": 103, "income": "-0.2", "asset": "BNB"},   # 非 USDT, 忽略
        ]
        runner = _make_runner(tmp_path, records, last_tran_file=100)
        runner._sync_funding_income()
        assert runner._funding_last_tran == 103
        assert abs(runner.portfolio.total_funding_fees - 0.5) < 1e-9

    def test_empty_response_fail_closed(self, tmp_path):
        runner = _make_runner(tmp_path, [], last_tran_file=100)
        runner._sync_funding_income()
        # 游标从状态文件加载后保持原值, 流水为空不记账 (fail-closed)
        assert runner._funding_last_tran == 100
        assert runner.portfolio.total_funding_fees == 0.0

    def test_estimate_accounting_skips_income_sync(self, tmp_path):
        runner = _make_runner(tmp_path, [{"tranId": 1, "income": "-1", "asset": "USDT"}],
                              accounting="estimate")
        runner._sync_funding_income()
        assert runner.portfolio.total_funding_fees == 0.0

    def test_gateway_get_income_parses_list(self, monkeypatch):
        gw = OrderGateway(testnet=True)
        monkeypatch.setattr(gw, "_request", lambda *a, **k: [
            {"tranId": 1, "income": "-0.5", "asset": "USDT"}])
        result = gw.get_income(start_time_ms=123)
        assert isinstance(result, list)
        assert result[0]["tranId"] == 1

    def test_gateway_get_income_bad_response(self, monkeypatch):
        gw = OrderGateway(testnet=True)
        monkeypatch.setattr(gw, "_request", lambda *a, **k: {"code": -2014, "msg": "x"})
        assert gw.get_income() is None  # 端点不可用 → None (可回退估算)

    def test_income_unavailable_falls_back_to_estimate(self, tmp_path):
        """income 端点不可用且从未成功对账 → FundingRateMonitor 回退估算记账。"""

        class _NoneGateway:
            def get_income(self, **kwargs):
                return None

        from shared.runner import SystemRunner
        runner = SystemRunner.__new__(SystemRunner)
        runner.execution_mode = MagicMock()
        runner.execution_mode.is_live.return_value = True
        runner.portfolio = PortfolioTracker(initial_equity=1000.0)
        runner.gateway = _NoneGateway()
        runner._funding_accounting = "income"
        runner._funding_state_path = str(tmp_path / "s.json")
        runner._funding_last_tran = None
        runner._estimate_fallback_on = False
        runner.funding_monitor = MagicMock()
        runner._sync_funding_income()
        assert runner._estimate_fallback_on is True
        assert runner.funding_monitor.on_cost == runner._on_funding_cost
        # 幂等: 第二次不再重复接线
        runner.funding_monitor.on_cost = "already_wired"
        runner._sync_funding_income()
        assert runner.funding_monitor.on_cost == "already_wired"

    def test_no_fallback_once_income_worked(self, tmp_path):
        """income 曾成功对账 (有游标) → 端点再故障不回退估算, 防双记账。"""
        records = [{"tranId": 5, "income": "-1", "asset": "USDT"}]
        runner = _make_runner(tmp_path, records, last_tran_file=4)
        runner.funding_monitor = MagicMock()
        runner._estimate_fallback_on = False
        runner._sync_funding_income()  # 成功对账 → last_tran=5
        runner.gateway = _FakeGateway(None)  # 模拟端点故障 (None)
        runner._sync_funding_income()
        assert runner._estimate_fallback_on is False
        assert runner.funding_monitor.on_cost != runner._on_funding_cost


# ─── #1/#2 自动减仓 / 回撤分级 ───

def _protective_runner():
    from shared.runner import SystemRunner
    runner = SystemRunner.__new__(SystemRunner)
    runner.execution_mode = MagicMock()
    runner.execution_mode.is_live.return_value = True
    runner.portfolio = PortfolioTracker(initial_equity=1000.0)
    runner.portfolio.open_position(Position("BTCUSDT", "LONG", 0.01, 60000.0, 3))  # 保证金 200
    runner.portfolio.open_position(Position("ETHUSDT", "LONG", 1.0, 3000.0, 3))    # 保证金 1000
    runner._last_deleverage_ts = 0.0
    runner._drawdown_reduce_armed = True
    runner._last_reduce_ts = 0.0
    runner._force_exit_symbol = MagicMock()
    runner._refresh_equity = MagicMock()
    runner._send_critical = MagicMock()
    return runner


class TestProtectiveClose:
    def test_margin_deleverage_closes_max_margin_position(self, monkeypatch):
        monkeypatch.setenv("MARGIN_DELEVERAGE_THRESHOLD", "0.8")
        monkeypatch.setenv("DRAWDOWN_REDUCE_TIER", "0")
        runner = _protective_runner()
        # margin_ratio = 1200/1000 = 1.2 > 0.8
        runner._protective_check()
        runner._force_exit_symbol.assert_called_once_with("ETHUSDT")
        runner._send_critical.assert_called_once()

    def test_no_action_below_threshold(self, monkeypatch):
        monkeypatch.setenv("MARGIN_DELEVERAGE_THRESHOLD", "1.5")
        monkeypatch.setenv("DRAWDOWN_REDUCE_TIER", "0")
        runner = _protective_runner()
        runner._protective_check()
        runner._force_exit_symbol.assert_not_called()

    def test_drawdown_tier_closes_once_then_rearms(self, monkeypatch):
        monkeypatch.setenv("MARGIN_DELEVERAGE_THRESHOLD", "0")
        monkeypatch.setenv("DRAWDOWN_REDUCE_TIER", "0.12")
        runner = _protective_runner()
        runner.portfolio.peak_equity = 2000.0  # 回撤 50%
        runner._protective_check()
        runner._force_exit_symbol.assert_called_once()
        runner._force_exit_symbol.reset_mock()
        runner._protective_check()  # 已 disarm → 不再关
        runner._force_exit_symbol.assert_not_called()
        # 回撤回落到档位 80% 以下 → 重新武装
        runner.portfolio.peak_equity = 1050.0  # 回撤 ~4.8% < 9.6%
        runner._protective_check()
        assert runner._drawdown_reduce_armed is True

    def test_paper_mode_skips(self, monkeypatch):
        monkeypatch.setenv("MARGIN_DELEVERAGE_THRESHOLD", "0.8")
        monkeypatch.setenv("DRAWDOWN_REDUCE_TIER", "0")
        runner = _protective_runner()
        runner.execution_mode.is_live.return_value = False
        runner._protective_check()
        runner._force_exit_symbol.assert_not_called()


# ─── #5 每日摘要 ───

class TestDailyDigest:
    def test_compose_digest_content(self):
        from shared.runner import SystemRunner
        runner = SystemRunner.__new__(SystemRunner)
        runner.stats = {"start_time": time.time() - 3600, "signals": 3,
                        "risk_rejected": 1, "orders_placed": 2, "orders_failed": 0,
                        "kline_closes": 96, "stalls": 0}
        runner._circuit_breaker = None
        runner.portfolio = PortfolioTracker(initial_equity=1234.5)
        runner.portfolio.open_position(Position("BTCUSDT", "LONG", 0.001, 60000.0, 3))
        runner.feed = MagicMock()
        runner.feed.get_last_price.return_value = 61000.0
        digest = runner._compose_digest()
        assert "每日运营摘要" in digest
        assert "BTCUSDT" in digest
        assert "1234.50" in digest

    def test_digest_once_per_day_window(self, monkeypatch):
        from shared.runner import SystemRunner
        import datetime
        runner = SystemRunner.__new__(SystemRunner)
        runner._last_digest_date = ""
        runner._last_digest_check = 0.0
        runner._dingtalk = None
        runner._ensure_dingtalk = lambda: None
        runner._compose_digest = lambda: "[SysTrader] 摘要"
        runner.event_bus = None
        runner.stats = {"start_time": time.time()}
        monkeypatch.setenv("DIGEST_ENABLED", "1")
        now = datetime.datetime.now()
        monkeypatch.setenv("DIGEST_HOUR", str(now.hour))
        monkeypatch.setenv("DIGEST_MINUTE", str(max(0, now.minute)))
        monkeypatch.setattr("shared.runner.datetime",
                            type("FakeDT", (), {"now": staticmethod(lambda: now)}))
        sent = []
        runner._dingtalk = MagicMock()
        runner._dingtalk.send = lambda msg: sent.append(msg) or True
        runner._maybe_send_daily_digest()
        assert len(sent) == 1
        runner._maybe_send_daily_digest()  # 同日去重
        assert len(sent) == 1


# ─── #10 盈亏平衡价 ───

class TestBreakEven:
    def test_collector_adds_break_even(self, monkeypatch):
        from dashboard.data_collector import DataCollector
        state = MagicMock()
        state.positions_snapshot.return_value = {
            "BTCUSDT": {"symbol": "BTCUSDT", "direction": "LONG",
                        "quantity": 0.001, "entry_price": 60000.0},
            "ETHUSDT": {"symbol": "ETHUSDT", "direction": "SHORT",
                        "quantity": 1.0, "entry_price": 3000.0},
        }
        state.assets = []
        state.available_balance = 100.0
        state.equity = 1000.0
        state.margin_ratio = 0.0
        state.daily_pnl = 0.0
        state.drawdown = 0.0
        state.signals = []
        state.orders = []
        state.heartbeats = {}
        feed = MagicMock()
        feed.get_mark_price.return_value = None
        feed.get_last_price.return_value = None
        dc = DataCollector(state, feed)
        # 屏蔽外部服务调用 (ticker/代理池/网络监控), 只测 positions 组装
        monkeypatch.setattr(dc, "_collect_tickers", lambda: [])
        monkeypatch.setattr(dc, "_collect_proxy_pool", lambda: {})
        monkeypatch.setattr(dc, "_collect_network", lambda: {})
        payload = dc.collect()
        by_symbol = {p["symbol"]: p for p in payload["positions"]}
        # LONG: 60000 * 1.001/0.999 ≈ 60120.12
        assert abs(by_symbol["BTCUSDT"]["break_even"] - 60120.12) < 0.5
        # SHORT: 3000 * 0.999/1.001 ≈ 2994.006
        assert abs(by_symbol["ETHUSDT"]["break_even"] - 2994.006) < 0.01

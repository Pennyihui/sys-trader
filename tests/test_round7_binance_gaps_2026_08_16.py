"""第七轮: Binance 合约 API 缺口补强测试 (2026-08-16)。

覆盖:
  #1 实际手续费率 (commissionRate → fee_rate)
  #2 清算价/爆仓距离/ADL (positionRisk v3)
  #3 ADL/保证金率告警 (MARGIN_CALL handler)
  #4 条件单触发基准 workingType
  #5 追踪止损 TRAILING_STOP_MARKET
  #6 多资产模式检测 + 可用余额口径修复
  #7 大额强平事件 (forceOrder)
  #8 限流权重 gauge (X-MBX-USED-WEIGHT-1M)
"""

import os
import time
from unittest.mock import MagicMock

import pytest

from execution.order_gateway import OrderGateway, AlgoOrderRequest
from execution.order_manager import OrderManager, OrderState
from portfolio.tracker import PortfolioTracker, Position
from shared.execution_mode import ExecutionMode, ExecutionModeManager


def _runner(**attrs):
    from shared.runner import SystemRunner
    r = SystemRunner.__new__(SystemRunner)
    r.instance = "live"
    for k, v in attrs.items():
        setattr(r, k, v)
    return r


# ─── #1 实际手续费率 ───

class TestFeeRate:
    def test_explicit_fee_rate_override(self, monkeypatch):
        monkeypatch.setenv("FEE_RATE", "0.0008")
        r = _runner(symbols=["BTCUSDT"])
        r.gateway = MagicMock()
        assert r._resolve_fee_rate() == 0.0008
        r.gateway.get_commission_rate.assert_not_called()

    def test_auto_uses_2x_max_taker(self, monkeypatch):
        monkeypatch.setenv("FEE_RATE", "auto")
        r = _runner(symbols=["BTCUSDT", "ETHUSDT"])
        gw = MagicMock()
        gw.get_commission_rate.side_effect = [
            {"symbol": "BTCUSDT", "makerCommissionRate": "0.0002",
             "takerCommissionRate": "0.0004"},
            {"symbol": "ETHUSDT", "makerCommissionRate": "0.0002",
             "takerCommissionRate": "0.0005"},
        ]
        r.gateway = gw
        assert r._resolve_fee_rate() == 0.001  # 2 × 0.0005

    def test_auto_fallback_on_failure(self, monkeypatch):
        monkeypatch.setenv("FEE_RATE", "auto")
        r = _runner(symbols=["BTCUSDT"])
        r.gateway = MagicMock()
        r.gateway.get_commission_rate.return_value = None
        assert r._resolve_fee_rate() == 0.001

    def test_gateway_parse(self, monkeypatch):
        gw = OrderGateway(testnet=True)
        monkeypatch.setattr(gw, "_request", lambda *a, **k: {
            "symbol": "BTCUSDT", "makerCommissionRate": "0.0002",
            "takerCommissionRate": "0.0004"})
        rate = gw.get_commission_rate("BTCUSDT")
        assert rate["takerCommissionRate"] == "0.0004"
        monkeypatch.setattr(gw, "_request", lambda *a, **k: {"code": -1100})
        assert gw.get_commission_rate("BTCUSDT") is None


# ─── #6 可用余额口径 ───

class TestAvailableBalance:
    def test_single_asset_uses_usdt(self):
        acc = {"assets": [{"asset": "USDT", "availableBalance": "900.5"},
                          {"asset": "BTC", "availableBalance": "0.01"}],
               "totalMarginBalance": "5000.0"}
        assert SystemRunner_available(acc, False) == 900.5

    def test_multi_assets_uses_total_margin_balance(self):
        acc = {"assets": [{"asset": "USDT", "availableBalance": "100.0"}],
               "totalMarginBalance": "5000.0"}
        assert SystemRunner_available(acc, True) == 5000.0

    def test_no_usdt_falls_back(self):
        acc = {"assets": [{"asset": "BTC", "availableBalance": "0.01"}],
               "totalMarginBalance": "5000.0"}
        assert SystemRunner_available(acc, False) == 5000.0


def SystemRunner_available(acc, multi):
    from shared.runner import SystemRunner
    return SystemRunner._available_balance(acc, multi)


# ─── #2/#3 清算价 / ADL ───

def _risk_runner():
    r = _runner()
    r.execution_mode = MagicMock()
    r.execution_mode.is_live.return_value = True
    r.portfolio = PortfolioTracker(initial_equity=1000.0)
    r.portfolio.open_position(Position("BTCUSDT", "LONG", 0.001, 60000.0, 3))
    r.event_bus = MagicMock()
    r._adl_warned = set()
    r._last_deleverage_ts = 0.0
    r._send_critical = MagicMock()
    r._protective_close = MagicMock()
    return r


class TestPositionRisks:
    def test_liquidation_distance_triggers_protective_close(self, monkeypatch):
        monkeypatch.setenv("LIQ_ALERT_PCT", "0.08")
        r = _risk_runner()
        r.gateway = MagicMock()
        r.gateway.get_position_risks.return_value = [
            {"symbol": "BTCUSDT", "liquidationPrice": "58000", "markPrice": "60000",
             "adlQuantile": 0}]
        r._sync_position_risks()
        r._protective_close.assert_called_once()  # 距离 3.3% < 8%

    def test_no_action_when_far_from_liquidation(self, monkeypatch):
        monkeypatch.setenv("LIQ_ALERT_PCT", "0.08")
        r = _risk_runner()
        r.gateway = MagicMock()
        r.gateway.get_position_risks.return_value = [
            {"symbol": "BTCUSDT", "liquidationPrice": "40000", "markPrice": "60000",
             "adlQuantile": 0}]
        r._sync_position_risks()
        r._protective_close.assert_not_called()

    def test_publishes_position_risk_event(self, monkeypatch):
        monkeypatch.setenv("LIQ_ALERT_PCT", "0")
        r = _risk_runner()
        r.gateway = MagicMock()
        r.gateway.get_position_risks.return_value = [
            {"symbol": "BTCUSDT", "liquidationPrice": "40000", "markPrice": "60000",
             "adlQuantile": 2}]
        r._sync_position_risks()
        published = [c for c in r.event_bus.publish.call_args_list
                     if c[0][0] == "position.risk"]
        assert published
        payload = published[0][0][1]
        assert payload["symbol"] == "BTCUSDT"
        assert payload["liquidation_price"] == 40000.0
        assert payload["adl_quantile"] == 2
        assert payload["liq_distance_pct"] == pytest.approx(1 / 3, abs=1e-4)

    def test_adl_warns_once_then_rearms(self, monkeypatch):
        monkeypatch.setenv("LIQ_ALERT_PCT", "0")
        r = _risk_runner()
        r.gateway = MagicMock()
        r.gateway.get_position_risks.return_value = [
            {"symbol": "BTCUSDT", "liquidationPrice": "40000", "markPrice": "60000",
             "adlQuantile": 3}]
        r._sync_position_risks()
        r._sync_position_risks()
        assert r._send_critical.call_count == 1  # ADL 只告警一次
        r.gateway.get_position_risks.return_value = [
            {"symbol": "BTCUSDT", "liquidationPrice": "40000", "markPrice": "60000",
             "adlQuantile": 0}]
        r._sync_position_risks()
        assert "BTCUSDT" not in r._adl_warned  # 退出队列 → 重新武装

    def test_fetch_failure_fail_closed(self, monkeypatch):
        monkeypatch.setenv("LIQ_ALERT_PCT", "0.08")
        r = _risk_runner()
        r.gateway = MagicMock()
        r.gateway.get_position_risks.return_value = None
        r._sync_position_risks()
        r._protective_close.assert_not_called()
        r.event_bus.publish.assert_not_called()

    def test_gateway_position_risks_parse(self, monkeypatch):
        gw = OrderGateway(testnet=True)
        monkeypatch.setattr(gw, "_request", lambda *a, **k: [
            {"symbol": "BTCUSDT", "liquidationPrice": "1.0"}])
        assert isinstance(gw.get_position_risks(), list)
        monkeypatch.setattr(gw, "_request", lambda *a, **k: {"code": -1100})
        assert gw.get_position_risks() is None

    def test_gateway_multi_assets_parse(self, monkeypatch):
        gw = OrderGateway(testnet=True)
        monkeypatch.setattr(gw, "_request", lambda *a, **k: {"multiAssetsMargin": True})
        assert gw.get_multi_assets_mode() is True
        monkeypatch.setattr(gw, "_request", lambda *a, **k: {"multiAssetsMargin": False})
        assert gw.get_multi_assets_mode() is False
        monkeypatch.setattr(gw, "_request", lambda *a, **k: {"code": -1100})
        assert gw.get_multi_assets_mode() is None


class TestMarginCall:
    def test_margin_call_sends_critical(self):
        r = _runner()
        r._send_critical = MagicMock()
        r._on_margin_call({"p": {"s": "BTCUSDT"}})
        r._send_critical.assert_called_once()
        assert "MARGIN CALL" in r._send_critical.call_args[0][0]


# ─── #7 大额强平告警 ───

class TestForceOrder:
    def test_below_threshold_ignored(self, monkeypatch):
        monkeypatch.setenv("FORCE_ORDER_ALERT_USDT", "100000")
        r = _runner()
        r._last_force_alert = {}
        r._dingtalk = MagicMock()
        r.event_bus = MagicMock()
        r._on_force_order({"o": {"s": "BTCUSDT", "S": "SELL", "q": "1", "p": "60000"}})
        r._dingtalk.send.assert_not_called()

    def test_large_liquidation_alerts(self, monkeypatch):
        monkeypatch.setenv("FORCE_ORDER_ALERT_USDT", "100000")
        r = _runner()
        r._last_force_alert = {}
        r._dingtalk = MagicMock()
        r.event_bus = MagicMock()
        r._on_force_order({"o": {"s": "BTCUSDT", "S": "SELL", "q": "2", "p": "60000"}})
        assert r._dingtalk.send.call_count == 1
        assert "120000" in r._dingtalk.send.call_args[0][0]
        # 5 分钟节流: 第二次同 symbol 不重复发
        r._on_force_order({"o": {"s": "BTCUSDT", "S": "SELL", "q": "2", "p": "60000"}})
        assert r._dingtalk.send.call_count == 1

    def test_disabled_by_zero_threshold(self, monkeypatch):
        monkeypatch.setenv("FORCE_ORDER_ALERT_USDT", "0")
        r = _runner()
        r._last_force_alert = {}
        r._dingtalk = MagicMock()
        r.event_bus = MagicMock()
        r._on_force_order({"o": {"s": "BTCUSDT", "S": "SELL", "q": "999", "p": "99999"}})
        r._dingtalk.send.assert_not_called()


# ─── #4/#5 workingType + 追踪止损 ───

def _capture_gateway():
    """捕获 place_algo_order 收到的 AlgoOrderRequest 参数。"""
    gw = MagicMock()
    gw.place_algo_order.side_effect = lambda req: MagicMock(
        algo_id=1, symbol=req.symbol, side=req.side, status="NEW", error=None)
    return gw


class TestWorkingTypeAndTrailing:
    def test_working_type_mark_price(self, monkeypatch):
        monkeypatch.setenv("PROTECTION_WORKING_TYPE", "MARK_PRICE")
        monkeypatch.setenv("PROTECTION_SL_MODE", "stop")
        gw = _capture_gateway()
        om = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode("live")))
        om.submit_stop_loss("BTCUSDT", "LONG", 0.001, 62000.0)
        req = gw.place_algo_order.call_args[0][0]
        assert req.working_type == "MARK_PRICE"
        assert req.order_type == "STOP_MARKET"

    def test_trailing_mode_submits_trailing_stop(self, monkeypatch):
        monkeypatch.setenv("PROTECTION_SL_MODE", "trailing")
        monkeypatch.setenv("TRAILING_STOP_CALLBACK", "2")
        monkeypatch.setenv("PROTECTION_WORKING_TYPE", "CONTRACT_PRICE")
        gw = _capture_gateway()
        om = OrderManager(gateway=gw, execution_mode=ExecutionModeManager(ExecutionMode("live")))
        order = om.submit_stop_loss("BTCUSDT", "LONG", 0.001, 62000.0)
        req = gw.place_algo_order.call_args[0][0]
        assert req.order_type == "TRAILING_STOP_MARKET"
        assert req.callback_rate == 2.0
        assert req.working_type == "CONTRACT_PRICE"
        assert order.order_type == "TRAILING_STOP_MARKET"
        assert order.state == OrderState.PENDING

    def test_trailing_falls_back_to_stop_in_paper(self, monkeypatch):
        monkeypatch.setenv("PROTECTION_SL_MODE", "trailing")
        gw = _capture_gateway()
        om = OrderManager(gateway=gw,
                          execution_mode=ExecutionModeManager(ExecutionMode("paper")),
                          paper_trader=MagicMock())
        om.submit_stop_loss("BTCUSDT", "LONG", 0.001, 62000.0)
        # PAPER 模式走 paper_trader.execute, 不走 algo API (无递归/不报错即通过)

    def test_gateway_passes_callback_and_working_type(self, monkeypatch):
        gw = OrderGateway(testnet=True)
        captured = {}
        monkeypatch.setattr(gw, "_request",
                            lambda m, e, p: captured.update(p) or {"algoId": 1})
        req = AlgoOrderRequest(symbol="BTCUSDT", side="SELL",
                               order_type="TRAILING_STOP_MARKET", quantity=0.001,
                               callback_rate=1.5, working_type="MARK_PRICE",
                               reduce_only=True)
        gw.place_algo_order(req)
        assert captured["callbackRate"] == "1.5"
        assert captured["workingType"] == "MARK_PRICE"
        assert captured["reduceOnly"] == "true"
        assert captured["type"] == "TRAILING_STOP_MARKET"


# ─── #8 限流权重 gauge ───

class TestApiWeightGauge:
    def test_weight_header_registers_gauge(self, monkeypatch):
        from monitor.collector import MetricsCollector
        MetricsCollector.reset()
        gw = OrderGateway(testnet=True)
        monkeypatch.setattr(gw, "_last_sync", time.time() + 9999)  # 跳过校时

        class _Resp:
            status_code = 200
            headers = {"X-MBX-USED-WEIGHT-1M": "42"}
            def json(self):
                return {}

        monkeypatch.setattr("requests.get", lambda *a, **k: _Resp())
        gw._request("GET", "/fapi/v1/time", {})
        assert MetricsCollector.instance().get_gauge("api_weight_used") == 42.0
        MetricsCollector.reset()


# ─── StateStore / DataCollector 联动 ───

class TestStateStoreRisk:
    def test_position_risk_stored_and_cleared(self):
        from dashboard.state_store import StateStore
        from shared.event_bus import Event
        store = StateStore(event_bus=MagicMock(), instance_filter="live")
        store._handle(Event(stream="position.risk", data={
            "instance": "live", "symbol": "BTCUSDT",
            "liquidation_price": 50000.0, "liq_distance_pct": 0.1,
            "adl_quantile": 0}))
        assert "BTCUSDT" in store.position_risks
        store._handle(Event(stream="position.changed", data={
            "instance": "live", "event": "close", "symbol": "BTCUSDT"}))
        assert "BTCUSDT" not in store.position_risks

    def test_equity_fee_rate_stored(self):
        from dashboard.state_store import StateStore
        from shared.event_bus import Event
        store = StateStore(event_bus=MagicMock(), instance_filter="live")
        store._handle(Event(stream="position.changed", data={
            "instance": "live", "event": "equity", "total_equity": 1000.0,
            "fee_rate": 0.0008}))
        assert store.fee_rate == 0.0008

    def test_collector_uses_state_fee_rate_and_risk(self, monkeypatch):
        from dashboard.data_collector import DataCollector
        state = MagicMock()
        state.positions_snapshot.return_value = {
            "BTCUSDT": {"symbol": "BTCUSDT", "direction": "LONG",
                        "quantity": 0.001, "entry_price": 60000.0}}
        state.fee_rate = 0.001
        state.position_risks = {
            "BTCUSDT": {"symbol": "BTCUSDT", "liquidation_price": 50000.0,
                        "liq_distance_pct": 0.1667, "adl_quantile": 0}}
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
        feed.get_mark_price.return_value = 60000.0
        feed.get_last_price.return_value = None
        dc = DataCollector(state, feed)
        monkeypatch.setattr(dc, "_collect_tickers", lambda: [])
        monkeypatch.setattr(dc, "_collect_proxy_pool", lambda: {})
        monkeypatch.setattr(dc, "_collect_network", lambda: {})
        payload = dc.collect()
        pos = payload["positions"][0]
        assert pos["liquidation_price"] == 50000.0
        assert pos["liq_distance_pct"] == 0.1667
        assert pos["adl_quantile"] == 0

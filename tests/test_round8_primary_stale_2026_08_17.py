"""第八轮: 主连接静默断流看护测试 (2026-08-17)。

背景: 24h 稳定性测试发现 closes 停在 168 达 18h, 但 ws=8/8、价格正常、
stalls=0 — 根因是主连接在代理节点半开状态下 TCP 存活、ping 保活正常、
on_close 不触发, 永不切主; 备用连接喂价格但 K 线闭合回调仅主连接触发。
本测试覆盖 feed.primary_stale_seconds / force_primary_switch 与
runner._check_connections 的看护接线。
"""

import time
from unittest.mock import MagicMock

import pytest

from market_data.feed import MarketDataFeed, _ConnState


def _feed_with_conns(redundant: int = 4):
    feed = MarketDataFeed(symbols=["BTCUSDT"], redundant_connections=redundant)
    feed._conns = [_ConnState(i) for i in range(redundant)]
    feed._primary_idx = 0
    return feed


class TestPrimaryStale:
    def test_returns_age_of_primary_last_message(self):
        feed = _feed_with_conns()
        feed._conns[0].last_msg_ts = time.time() - 5
        feed._conns[1].last_msg_ts = time.time()
        assert feed.primary_stale_seconds() == pytest.approx(5.0, abs=0.5)

    def test_unknown_when_no_message_recorded(self):
        feed = _feed_with_conns()
        assert feed.primary_stale_seconds() == -1.0

    def test_force_switch_closes_primary_ws(self):
        feed = _feed_with_conns()
        feed._conns[0].ws = MagicMock()
        feed._conns[1].connected = True
        feed.force_primary_switch()
        feed._conns[0].ws.close.assert_called_once()

    def test_force_switch_without_ws_triggers_switch(self, monkeypatch):
        feed = _feed_with_conns()
        feed._conns[0].ws = None
        called = []
        monkeypatch.setattr(feed, "_try_switch_primary",
                            lambda idx: called.append(idx))
        feed.force_primary_switch()
        assert called == [0]


class TestTimeSyncFix:
    """-1022 根因修复: 校时往返延迟剔除 + 超限幅 (2026-08-17)。

    旧实现把代理尖峰 RTT (可达 8s) 误算成时钟偏移 → 签名时间戳失真 →
    后续请求批量 -1022 Signature not valid。
    """

    def test_normal_offset_updates(self, monkeypatch):
        from execution.order_gateway import OrderGateway
        gw = OrderGateway(testnet=True)
        base = [time.time() * 1000]

        class _Resp:
            def json(self):
                return {"serverTime": int(base[0] + 150)}

        monkeypatch.setattr("requests.get", lambda *a, **k: _Resp())
        monkeypatch.setattr(gw, "_record_offset", lambda *a: None)
        gw._sync_server_time()
        assert 0 < gw._time_offset <= 5000

    def test_absurd_offset_clamped_keeps_old(self, monkeypatch):
        from execution.order_gateway import OrderGateway
        gw = OrderGateway(testnet=True)
        gw._time_offset = 123
        base = [time.time() * 1000]

        class _Resp:
            def json(self):
                return {"serverTime": int(base[0] + 8000)}  # 8s 假偏移

        monkeypatch.setattr("requests.get", lambda *a, **k: _Resp())
        monkeypatch.setattr(gw, "_record_offset", lambda *a: None)
        gw._sync_server_time()
        assert gw._time_offset == 123  # 限幅: 保留旧值

    def test_failure_keeps_last_offset(self, monkeypatch):
        from execution.order_gateway import OrderGateway
        gw = OrderGateway(testnet=True)
        gw._time_offset = 456

        def boom(*a, **k):
            raise ConnectionError("proxy EOF")

        monkeypatch.setattr("requests.get", boom)
        gw._sync_server_time()
        assert gw._time_offset == 456


class TestRunnerGuard:
    def _runner(self):
        from shared.runner import SystemRunner
        r = SystemRunner.__new__(SystemRunner)
        r.feed = MagicMock()
        r.feed._conns = [MagicMock()]
        r.feed._conns[0].connected = True
        return r

    def test_switches_when_primary_stale(self):
        r = self._runner()
        r.feed.primary_stale_seconds.return_value = 200.0
        r._check_connections()
        r.feed.force_primary_switch.assert_called_once()

    def test_no_switch_when_fresh(self):
        r = self._runner()
        r.feed.primary_stale_seconds.return_value = 10.0
        r._check_connections()
        r.feed.force_primary_switch.assert_not_called()

    def test_no_switch_when_unknown(self):
        r = self._runner()
        r.feed.primary_stale_seconds.return_value = -1.0
        r._check_connections()
        r.feed.force_primary_switch.assert_not_called()

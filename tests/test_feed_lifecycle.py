"""测试 MarketDataFeed 冗余连接架构的连接生命周期。"""
import json
import pytest
import threading
import time
from unittest.mock import MagicMock, patch, call
from market_data.feed import MarketDataFeed


@pytest.mark.integration
class TestFeedConnectionLifecycle:
    """4 连接冗余架构的启停、切换、竞态测试"""

    def setup_method(self):
        self.feed = MarketDataFeed(
            symbols=["BTCUSDT"],
            proxy_host="127.0.0.1",
            proxy_port=7897,
            redundant_connections=3,
        )

    def test_start_creates_correct_number_of_connections(self):
        """start() 应创建指定数量的连接"""
        self.feed.start()
        assert len(self.feed._conns) == 3
        assert all(c.thread is not None for c in self.feed._conns)
        self.feed.stop()

    def test_stop_clears_connections(self):
        """stop() 后 connection 列表应清空"""
        self.feed.start()
        time.sleep(0.1)
        self.feed.stop()
        assert len(self.feed._conns) == 0

    def test_double_start_is_idempotent(self):
        """重复 start() 不创建多余连接"""
        self.feed.start()
        count1 = len(self.feed._conns)
        self.feed.start()
        count2 = len(self.feed._conns)
        assert count1 == count2 == 3
        self.feed.stop()

    def test_primary_idx_starts_at_zero(self):
        """主连接索引初始为 0"""
        self.feed.start()
        assert self.feed._primary_idx == 0
        self.feed.stop()

    def test_all_connections_marked_connected_after_open(self):
        """所有连接 _on_conn_open 后 connected 应为 True"""
        self.feed._conns = [MagicMock() for _ in range(3)]
        for i in range(3):
            self.feed._on_conn_open(i)
            assert self.feed._conns[i].connected is True

    def test_switch_primary_ignores_same_connection(self):
        """当 failed_idx 不等于 _primary_idx 时不应切换"""
        self.feed._primary_idx = 0
        self.feed._conns = [MagicMock() for _ in range(3)]
        self.feed._conns[0].connected = False
        self.feed._try_switch_primary(2)
        assert self.feed._primary_idx == 0

    def test_switch_primary_to_available_standby(self):
        """主连接断开时切换到第一个可用的备用连接"""
        self.feed._primary_idx = 0
        self.feed._conns = [MagicMock() for _ in range(3)]
        self.feed._conns[0].connected = False
        self.feed._conns[1].connected = False
        self.feed._conns[2].connected = True
        self.feed._try_switch_primary(0)
        assert self.feed._primary_idx == 2

    def test_on_message_wrapper_processes_all_connections(self):
        """所有连接的消息都应处理（备用连接维持共享 buffer），仅主连接触发回调"""
        self.feed._primary_idx = 0
        calls = []
        self.feed._on_message = lambda raw, notify_closed=True: calls.append(
            (raw, notify_closed)
        )
        self.feed._on_message_wrapper(0, "data1")
        self.feed._on_message_wrapper(1, "data2")
        assert len(calls) == 2
        assert ("data1", True) in calls
        assert ("data2", False) in calls

    def test_on_message_wrapper_no_lock_contention(self):
        """并发消息不应导致锁竞争崩溃"""
        self.feed._primary_idx = 0
        errors = []
        def stomp():
            try:
                for _ in range(100):
                    self.feed._on_message_wrapper(0, "{}")
            except Exception as e:
                errors.append(e)
        threads = [threading.Thread(target=stomp) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        assert len(errors) == 0


@pytest.mark.integration
class TestFeedFailover:
    """故障转移场景测试"""

    def setup_method(self):
        self.feed = MarketDataFeed(
            symbols=["BTCUSDT"],
            proxy_host="127.0.0.1",
            proxy_port=7897,
            redundant_connections=2,
        )

    def test_primary_failover_switches_conn(self):
        """主连接断开后应切换到备用连接"""
        self.feed._conns = [MagicMock() for _ in range(2)]
        self.feed._conns[0].connected = False
        self.feed._conns[1].connected = True
        self.feed._primary_idx = 0
        self.feed._try_switch_primary(0)
        assert self.feed._primary_idx == 1

    def test_connected_state_not_overwritten_after_reconnect(self):
        """重连后 connected 不应被旧状态覆盖"""
        state = MagicMock()
        state.connected = False
        self.feed._conns = [state]
        self.feed._on_conn_open(0)
        assert state.connected is True

    def test_on_message_wrapper_late_standby_message_no_callback(self):
        """切换主连接后，旧主连接的延迟消息只更新 buffer，不触发闭合回调"""
        self.feed._primary_idx = 1
        calls = []
        self.feed._on_message = lambda raw, notify_closed=True: calls.append(
            (raw, notify_closed)
        )
        self.feed._on_message_wrapper(0, "stale")
        assert len(calls) == 1
        assert calls[0] == ("stale", False)

    def test_closed_callback_fires_once_when_standby_writes_first(self):
        """备用连接先写入闭合 K 线到共享 buffer，主连接仍应触发且只触发一次回调"""
        self.feed._primary_idx = 0
        fired = []
        self.feed.on_kline_closed = lambda sym, tf, ohlcv: fired.append((sym, tf))
        msg = {
            "e": "kline", "s": "BTCUSDT",
            "k": {"t": 1700000000000, "T": 1700014400000, "o": "62000.0",
                  "h": "63000.0", "l": "61500.0", "c": "62500.0",
                  "v": "100.5", "x": True},
        }
        # 备用连接 (notify_closed=False) 先写入 buffer
        self.feed._on_message(json.dumps(msg), notify_closed=False)
        assert self.feed.buffer.is_closed("BTCUSDT", "4h", 1700000000000) is True
        # 主连接收到同一根闭合 K 线，应触发一次回调
        self.feed._on_message(json.dumps(msg), notify_closed=True)
        assert len(fired) == 1
        # 重复消息不重复触发
        self.feed._on_message(json.dumps(msg), notify_closed=True)
        assert len(fired) == 1

    def test_open_close_bounds_check(self):
        """_on_conn_open/close 应做边界检查"""
        self.feed._conns = []  # 空列表
        self.feed._on_conn_open(0)   # 不应 IndexError
        self.feed._on_conn_close(0, None, "")  # 不应 IndexError


@pytest.mark.integration
class TestFeedStopRace:
    """stop() 时的竞态测试"""

    def setup_method(self):
        self.feed = MarketDataFeed(
            symbols=["BTCUSDT"],
            proxy_host="127.0.0.1",
            proxy_port=7897,
            redundant_connections=2,
        )

    def test_stop_with_no_connections(self):
        """无连接时 stop() 不报错"""
        self.feed._conns = []
        self.feed.stop()

    def test_stop_while_close_callback_pending(self):
        """stop() 后延迟 callback 不引起 IndexError"""
        self.feed._conns = [MagicMock() for _ in range(2)]
        for c in self.feed._conns:
            c.ws = MagicMock()
            c.thread = MagicMock()
            c.thread.is_alive.return_value = False
        self.feed.stop()
        assert len(self.feed._conns) == 0

    def test_stop_idempotent(self):
        """重复 stop() 不报错"""
        self.feed.stop()
        self.feed.stop()

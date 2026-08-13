import json
import time
import uuid
import pytest
import redis
from unittest.mock import patch, MagicMock
from shared.event_bus import EventBus, Event


@pytest.mark.unit
class TestEventBus:
    def setup_method(self):
        self.bus = EventBus(redis_url="redis://localhost:6379", prefix="test")

    def test_publish_sends_message_to_stream(self):
        bus = self.bus
        data = {"symbol": "BTCUSDT", "price": 62500.0}

        with patch.object(bus.redis, "xadd") as mock_xadd:
            mock_xadd.return_value = "12345-0"
            event_id = bus.publish("test.stream", data)

        mock_xadd.assert_called_once()
        args = mock_xadd.call_args
        assert "test:test.stream" in args[0][0] or args[0][0] == "test:test.stream"
        assert event_id is not None

    def test_event_has_required_fields(self):
        event = Event(stream="signal.generated", data={"symbol": "BTCUSDT", "direction": "LONG"})

        assert isinstance(event.event_id, str)
        assert len(event.event_id) > 0
        assert event.stream == "signal.generated"
        assert event.data["symbol"] == "BTCUSDT"
        assert event.data["direction"] == "LONG"
        assert isinstance(event.timestamp, str)

    def test_subscribe_reads_from_stream(self):
        bus = self.bus
        handler_called = []

        def handler(event):
            handler_called.append(event)

        test_event = Event(stream="kline.closed", data={"symbol": "BTCUSDT", "timeframe": "4h"})
        raw = json.dumps({"event_id": test_event.event_id, "stream": test_event.stream, "timestamp": test_event.timestamp, "data": test_event.data})

        with patch.object(bus.redis, "xreadgroup") as mock_read, patch.object(bus.redis, "xack") as mock_xack:
            mock_read.return_value = [[b"test:kline.closed", [(b"msg-1", {b"payload": raw.encode()})]]]
            bus._poll_once("kline.closed", "test-group", handler)

        assert len(handler_called) == 1
        assert handler_called[0].stream == "kline.closed"
        assert handler_called[0].data["symbol"] == "BTCUSDT"

    def test_message_is_valid_json_roundtrip(self):
        original = Event(stream="order.filled", data={"symbol": "ETHUSDT", "qty": 0.5, "price": 3100.0})
        raw = json.dumps({"event_id": original.event_id, "stream": original.stream, "timestamp": original.timestamp, "data": original.data})
        parsed = json.loads(raw)

        assert parsed["event_id"] == original.event_id
        assert parsed["stream"] == original.stream
        assert parsed["data"]["symbol"] == "ETHUSDT"
        assert parsed["data"]["qty"] == 0.5

    def test_run_consumer_creates_group(self):
        """run_consumer 自动创建 consumer group（幂等，BUSYGROUP 容忍）。"""
        bus = self.bus
        with patch.object(bus.redis, "xgroup_create") as mock_create, \
                patch.object(bus, "_poll_once"), \
                patch.object(bus._stop, "is_set", side_effect=[False, True]):
            bus.run_consumer("test.stream", "grp", lambda e: None)

        mock_create.assert_called_once()
        args = mock_create.call_args
        assert args[0][0] == "test:test.stream"
        assert args[0][1] == "grp"
        assert args[1]["id"] == "$"
        assert args[1]["mkstream"] is True

        # BUSYGROUP 已存在时不抛异常，继续消费
        with patch.object(bus.redis, "xgroup_create",
                          side_effect=redis.ResponseError("BUSYGROUP Consumer group name already exists")), \
                patch.object(bus, "_poll_once"), \
                patch.object(bus._stop, "is_set", side_effect=[False, True]):
            bus.run_consumer("test.stream", "grp", lambda e: None)

    def test_run_consumer_retries_group_creation(self):
        """Redis 启动不可用时建组失败不杀死线程，重试成功后继续消费。"""
        bus = self.bus
        with patch.object(bus.redis, "xgroup_create",
                          side_effect=[redis.ConnectionError("Redis is down"), None]) as mock_create, \
                patch.object(bus, "_poll_once") as mock_poll, \
                patch.object(bus._stop, "is_set", side_effect=[False, False, True]), \
                patch.object(bus._stop, "wait") as mock_wait:
            bus.run_consumer("test.stream", "grp", lambda e: None)

        assert mock_create.call_count == 2  # 首次 ConnectionError，重试成功
        assert mock_poll.call_count == 1   # 建组成功后继续消费
        assert mock_wait.call_count == 1   # 失败后短暂等待再重试

    def test_publish_survives_redis_down(self):
        """Redis 不可用时 publish 不抛异常，返回空字符串。"""
        bus = EventBus(redis_url="redis://127.0.0.1:1")  # 必然失败的端口
        bus.redis.close()  # 强制断连
        assert bus.publish("test.stream", {"k": "v"}) == ""

    def test_consumer_id_is_stable_across_polls(self):
        """_poll_once 复用固定 consumer_id, 不每次生成新 consumer (防 consumer 膨胀)。"""
        bus = self.bus
        with patch.object(bus.redis, "xreadgroup") as mock_read, \
                patch.object(bus.redis, "xack"), \
                patch.object(bus.redis, "xautoclaim", return_value=([], "0-0")):
            mock_read.return_value = []
            bus._poll_once("kline.closed", "grp", lambda e: None)
            cid1 = mock_read.call_args[0][1]
            bus._poll_once("kline.closed", "grp", lambda e: None)
            cid2 = mock_read.call_args[0][1]
        assert cid1 == cid2
        assert cid1.startswith("grp-")

    def test_handler_exception_still_acks(self):
        """handler 抛异常时消息仍 ACK 并记日志, 避免 PEL 永久滞留。"""
        bus = self.bus
        raw = json.dumps({"event_id": "e1", "stream": "kline.closed",
                          "timestamp": "t", "data": {"symbol": "BTCUSDT"}})
        with patch.object(bus.redis, "xreadgroup") as mock_read, \
                patch.object(bus.redis, "xack") as mock_xack, \
                patch.object(bus.redis, "xautoclaim", return_value=([], "0-0")):
            mock_read.return_value = [[b"test:kline.closed", [(b"msg-1", {b"payload": raw.encode()})]]]

            def boom(event):
                raise RuntimeError("handler boom")

            bus._poll_once("kline.closed", "grp", boom)
        mock_xack.assert_called_once()
        assert mock_xack.call_args[0][2] == b"msg-1"

    def test_malformed_payload_still_acks(self):
        """payload 非 JSON 时同样 ACK 并记日志, 不永久滞留 PEL。"""
        bus = self.bus
        with patch.object(bus.redis, "xreadgroup") as mock_read, \
                patch.object(bus.redis, "xack") as mock_xack, \
                patch.object(bus.redis, "xautoclaim", return_value=([], "0-0")):
            mock_read.return_value = [[b"test:kline.closed", [(b"msg-2", {b"payload": b"not-json"})]]]
            bus._poll_once("kline.closed", "grp", lambda e: None)
        mock_xack.assert_called_once()
        assert mock_xack.call_args[0][2] == b"msg-2"

    def test_retry_pending_reclaims_and_delivers(self):
        """XAUTOCLAIM 捞回 PEL 滞留消息并交给 handler (重投)。"""
        bus = self.bus
        raw = json.dumps({"event_id": "e1", "stream": "kline.closed",
                          "timestamp": "t", "data": {"symbol": "BTCUSDT"}})
        delivered = []
        with patch.object(bus.redis, "xreadgroup") as mock_read, \
                patch.object(bus.redis, "xack") as mock_xack, \
                patch.object(bus.redis, "xautoclaim",
                             return_value=["0-0", [(b"msg-pending", {b"payload": raw.encode()})], []]):
            mock_read.return_value = []
            bus._poll_once("kline.closed", "grp", delivered.append)
        assert len(delivered) == 1
        assert delivered[0].data["symbol"] == "BTCUSDT"
        mock_xack.assert_called_once()

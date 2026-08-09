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

    def test_publish_survives_redis_down(self):
        """Redis 不可用时 publish 不抛异常，返回空字符串。"""
        bus = EventBus(redis_url="redis://127.0.0.1:1")  # 必然失败的端口
        bus.redis.close()  # 强制断连
        assert bus.publish("test.stream", {"k": "v"}) == ""

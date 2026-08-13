"""Event Bus backed by Redis Streams — module communication backbone."""

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
import redis

logger = logging.getLogger(__name__)


@dataclass
class Event:
    stream: str
    data: dict
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class EventBus:
    def __init__(self, redis_url: str = "redis://localhost:6379", prefix: str = "systrader"):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.prefix = prefix
        self._stop = threading.Event()
        # 固定 consumer 后缀: 每次 _poll_once 新生成 consumer_id 会让 XREADGROUP
        # 自动创建新 consumer, 一天可膨胀 ~86 万, 改为实例级单例复用同一 id。
        self._consumer_suffix = uuid.uuid4().hex[:8]

    def _key(self, stream: str) -> str:
        return f"{self.prefix}:{stream}"

    def _consumer_id(self, consumer_group: str) -> str:
        return f"{consumer_group}-{self._consumer_suffix}"

    def publish(self, stream: str, data: dict) -> str:
        event = Event(stream=stream, data=data)
        payload = json.dumps({"event_id": event.event_id, "stream": event.stream, "timestamp": event.timestamp, "data": event.data})
        try:
            msg_id = self.redis.xadd(self._key(stream), {"payload": payload}, maxlen=10000)
            return msg_id
        except Exception as e:
            logger.warning("EventBus publish failed [%s]: %s", stream, e)
            return ""

    def subscribe(self, stream: str, consumer_group: str, handler: Callable[[Event], None], count: int = 5, block: int = 100):
        key = self._key(stream)
        try:
            self.redis.xgroup_create(key, consumer_group, id="$", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        self.run_consumer(stream, consumer_group, handler, count=count, block=block)

    def _poll_once(self, stream: str, consumer_group: str, handler: Callable[[Event], None], count: int = 5, block: int = 100):
        key = self._key(stream)
        consumer_id = self._consumer_id(consumer_group)
        # 先捞回 PEL 中滞留的 pending 消息重试, 再读新消息
        self._retry_pending(key, stream, consumer_group, consumer_id, handler, count=count)
        results = self.redis.xreadgroup(consumer_group, consumer_id, {key: ">"}, count=count, block=block)
        if results:
            for _stream_key, messages in results:
                for msg_id, fields in messages:
                    self._deliver(stream, consumer_group, key, msg_id, fields, handler)

    def _retry_pending(self, key: str, stream: str, consumer_group: str, consumer_id: str,
                       handler: Callable[[Event], None], count: int = 5, min_idle_ms: int = 30000):
        """XAUTOCLAIM 捞回 PEL 中滞留的 pending 消息重试。

        上次消费失败未 ACK 的消息留在 PEL, ">" 只读新消息导致其永不重投;
        此处按 min-idle-time 捞回重试, 避免永久滞留。Redis 不可达或版本
        不支持 (无 xautoclaim) 时静默降级为只读新消息。
        """
        try:
            xautoclaim = getattr(self.redis, "xautoclaim", None)
            if xautoclaim is None:
                return
            result = xautoclaim(key, consumer_group, consumer_id, min_idle_ms, "0", count=count)
            # redis-py>=6 返回 [cursor, claimed_messages, deleted_ids]; 旧版 [messages, cursor]
            if isinstance(result, (list, tuple)) and len(result) >= 2:
                pending = result[1] if isinstance(result[1], (list, tuple)) else result[0]
            else:
                pending = []
            for msg_id, fields in pending:
                self._deliver(stream, consumer_group, key, msg_id, fields, handler)
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                return  # 消费组尚未建立 (run_consumer 正在建组), 下一轮再捞
            logger.warning("EventBus reclaim failed [%s/%s]: %s", stream, consumer_group, e)
        except Exception as e:
            logger.warning("EventBus reclaim failed [%s/%s]: %s", stream, consumer_group, e)

    def _deliver(self, stream: str, consumer_group: str, key: str, msg_id, fields: dict,
                 handler: Callable[[Event], None]):
        """处理单条消息: 解析或回调异常捕获后记日志并 ACK, 避免消息永久滞留 PEL。

        消费失败的消息经 _retry_pending 重试一次后仍失败则 ACK 丢弃并记日志,
        防止其永不重投地留在 PEL。
        """
        try:
            fields = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in fields.items()}
            payload = json.loads(fields.get("payload", "{}"))
            event = Event(stream=payload.get("stream", stream), data=payload.get("data", {}), event_id=payload.get("event_id", ""), timestamp=payload.get("timestamp", ""))
            handler(event)
        except Exception as e:
            logger.error("EventBus consume failed [%s/%s] msg=%s: %s", stream, consumer_group, msg_id, e)
        finally:
            try:
                self.redis.xack(key, consumer_group, msg_id)
            except Exception as e:
                logger.warning("EventBus xack failed [%s/%s] msg=%s: %s", stream, consumer_group, msg_id, e)

    def run_consumer(self, stream: str, consumer_group: str, handler: Callable[[Event], None], count: int = 5, block: int = 100):
        key = self._key(stream)
        while not self._stop.is_set():
            try:
                try:
                    self.redis.xgroup_create(key, consumer_group, id="$", mkstream=True)
                except redis.ResponseError as e:
                    if "BUSYGROUP" not in str(e):
                        raise
                self._poll_once(stream, consumer_group, handler, count=count, block=block)
            except Exception as e:
                logger.error("EventBus consumer error [%s/%s]: %s", stream, consumer_group, e)
                self._stop.wait(timeout=1)

    def stop(self):
        self._stop.set()

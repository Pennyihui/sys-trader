"""Event Bus backed by Redis Streams — module communication backbone."""

import json
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
import redis


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
        self._consumers: dict[str, list] = {}

    def _key(self, stream: str) -> str:
        return f"{self.prefix}:{stream}"

    def publish(self, stream: str, data: dict) -> str:
        event = Event(stream=stream, data=data)
        payload = json.dumps({"event_id": event.event_id, "stream": event.stream, "timestamp": event.timestamp, "data": event.data})
        msg_id = self.redis.xadd(self._key(stream), {"payload": payload}, maxlen=10000)
        return msg_id

    def subscribe(self, stream: str, consumer_group: str, handler: Callable[[Event], None]):
        key = self._key(stream)
        try:
            self.redis.xgroup_create(key, consumer_group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        if stream not in self._consumers:
            self._consumers[stream] = []
        self._consumers[stream].append((consumer_group, handler))

    def _poll_once(self, stream: str, consumer_group: str, handler: Callable[[Event], None]):
        key = self._key(stream)
        consumer_id = f"{consumer_group}-{uuid.uuid4().hex[:8]}"
        results = self.redis.xreadgroup(consumer_group, key, {key: ">"}, count=5, block=100)
        if results:
            for _stream_key, messages in results:
                for msg_id, fields in messages:
                    fields = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in fields.items()}
                    payload = json.loads(fields.get("payload", "{}"))
                    event = Event(stream=payload.get("stream", stream), data=payload.get("data", {}), event_id=payload.get("event_id", ""), timestamp=payload.get("timestamp", ""))
                    handler(event)
                    self.redis.xack(key, consumer_group, msg_id)

    def run_consumer(self, stream: str, consumer_group: str, handler: Callable[[Event], None]):
        import time
        while True:
            try:
                self._poll_once(stream, consumer_group, handler)
            except Exception as e:
                print(f"EventBus consumer error [{stream}/{consumer_group}]: {e}")
                time.sleep(1)

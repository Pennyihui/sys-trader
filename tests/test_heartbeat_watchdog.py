"""HeartbeatWatchdog 测试 — 判定逻辑与 Redis 解耦（注入 FakeRedis）。"""

import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from tools.heartbeat_watchdog import HEARTBEAT_STREAM, HeartbeatWatchdog, build_notifier


class FakeRedis:
    """xrevrange 返回固定 entries 的假 Redis（无 decode_responses 语义，可注入 bytes）。"""

    def __init__(self, entries):
        self.entries = entries

    def xrevrange(self, *args, **kwargs):
        return self.entries


def make_entry(timestamp_iso, payload_bytes: bool = False):
    """构造一条 heartbeat 流消息: [msg_id, {"payload": json}]。"""
    payload = json.dumps({
        "event_id": "evt-1", "stream": "heartbeat",
        "timestamp": timestamp_iso, "data": {"instance": "live", "modules": {}},
    })
    if payload_bytes:
        payload = payload.encode("utf-8")
    return ["1786545281775-0", {"payload": payload}]


def iso_hours_ago(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def fresh_iso(seconds_ago: float = 2.0) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()


@pytest.mark.unit
def test_last_event_age_parses_payload_timestamp():
    """payload 的 ISO timestamp（EventBus envelope 字段）→ age 正确。"""
    watchdog = HeartbeatWatchdog(redis_client=FakeRedis([make_entry(iso_hours_ago(0.1))]))
    age = watchdog.last_event_age()
    assert 350.0 < age < 370.0  # 0.1h = 360s 前


@pytest.mark.unit
def test_payload_bytes_are_decoded():
    """xrevrange fields 值为 bytes（decode_responses=False）时仍能解析。"""
    watchdog = HeartbeatWatchdog(redis_client=FakeRedis([make_entry(fresh_iso(), payload_bytes=True)]))
    age = watchdog.last_event_age()
    assert 0.0 <= age < 5.0


@pytest.mark.unit
def test_empty_stream_means_stale():
    """流为空（或不存在）→ age 无限大，由 check_once 判定为停滞。"""
    watchdog = HeartbeatWatchdog(redis_client=FakeRedis([]))
    assert watchdog.last_event_age() == float("inf")
    assert watchdog.check_once() == "stale"


@pytest.mark.unit
def test_malformed_payload_means_stale():
    """payload 非 JSON / 时间戳缺失 → fail-safe 视为停滞。"""
    watchdog = HeartbeatWatchdog(redis_client=FakeRedis([["1-0", {"payload": "not-json"}]]))
    assert watchdog.last_event_age() == float("inf")
    bad_ts = FakeRedis([make_entry("not-a-timestamp")])
    assert HeartbeatWatchdog(redis_client=bad_ts).last_event_age() == float("inf")


@pytest.mark.unit
def test_stale_triggers_alert_once():
    """状态机: 连续 stale 轮次只告警一次（进入停滞时告警，之后不重复）。"""
    notifier = MagicMock()
    watchdog = HeartbeatWatchdog(
        redis_client=FakeRedis([make_entry(iso_hours_ago(1))]),
        stale_after=60.0, notifier=notifier)
    assert watchdog.check_once() == "stale"
    watchdog.check_once()
    watchdog.check_once()
    assert notifier.send.call_count == 1
    msg = notifier.send.call_args[0][0]
    assert "心跳停滞" in msg and "3600s" in msg


@pytest.mark.unit
def test_recovery_notifies():
    """stale → 恢复时发恢复通知一次，回到 normal 后不再通知。"""
    notifier = MagicMock()
    fake = FakeRedis([make_entry(iso_hours_ago(1))])
    watchdog = HeartbeatWatchdog(
        redis_client=fake, stale_after=60.0, notifier=notifier)
    assert watchdog.check_once() == "stale"
    assert notifier.send.call_count == 1

    fake.entries = [make_entry(fresh_iso())]  # 流恢复
    assert watchdog.check_once() == "recovered"   # 恢复通知
    assert notifier.send.call_count == 2
    assert "恢复" in notifier.send.call_args[0][0]

    assert watchdog.check_once() == "normal"       # 回到 normal，无新通知
    assert notifier.send.call_count == 2


@pytest.mark.unit
def test_re_stale_alerts_again():
    """recovered → 再次停滞 → 重新告警（新停滞周期）。"""
    notifier = MagicMock()
    fake = FakeRedis([make_entry(iso_hours_ago(1))])
    watchdog = HeartbeatWatchdog(redis_client=fake, stale_after=60.0, notifier=notifier)
    watchdog.check_once()                      # stale, 告警 #1
    fake.entries = [make_entry(fresh_iso())]
    watchdog.check_once()                      # recovered, 恢复通知 #2
    fake.entries = [make_entry(iso_hours_ago(2))]
    assert watchdog.check_once() == "stale"    # 再次停滞, 告警 #3
    assert notifier.send.call_count == 3


@pytest.mark.unit
def test_no_webhook_degrades_to_log(monkeypatch, caplog):
    """webhook 未配置: build_notifier 返回 None，停滞告警走 logger 不崩溃。"""
    monkeypatch.delenv("DINGTALK_WEBHOOK_URL", raising=False)
    assert build_notifier() is None

    watchdog = HeartbeatWatchdog(
        redis_client=FakeRedis([make_entry(iso_hours_ago(1))]),
        stale_after=60.0, notifier=None)
    with caplog.at_level(logging.WARNING):
        assert watchdog.check_once() == "stale"
    assert any("心跳停滞" in r.message for r in caplog.records)


@pytest.mark.unit
def test_build_notifier_with_webhook(monkeypatch):
    """webhook + secret 配置齐全时构造 DingTalkNotifier。"""
    monkeypatch.setenv("DINGTALK_WEBHOOK_URL", "https://oapi.dingtalk.com/robot/send?access_token=t")
    monkeypatch.setenv("DINGTALK_SECRET", "SECxxx")
    notifier = build_notifier()
    assert notifier is not None
    assert notifier.webhook_url.startswith("https://oapi.dingtalk.com")
    assert notifier.secret == "SECxxx"


@pytest.mark.unit
def test_stream_key_defaults_to_systrader_heartbeat():
    assert HEARTBEAT_STREAM == "systrader:heartbeat"
    watchdog = HeartbeatWatchdog(redis_client=FakeRedis([make_entry(fresh_iso())]))
    watchdog.last_event_age()
    assert watchdog.stream_key == "systrader:heartbeat"

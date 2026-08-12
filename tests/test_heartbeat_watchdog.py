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


# ─── Ops T5: K线闭合停滞 + 订单失败率 (stats 维度) ───


def make_stats_entry(timestamp_iso, stats: dict):
    """带 stats 字段的 heartbeat 消息 (与真实 EventBus envelope 一致: stats 在 data 内)。"""
    payload = json.dumps({
        "event_id": "evt-1", "stream": "heartbeat",
        "timestamp": timestamp_iso,
        "data": {"instance": "live", "modules": {}, "stats": stats},
    })
    return ["1786545281775-0", {"payload": payload}]


def _watchdog_with_notifier(fake, **kw):
    notifier = MagicMock()
    return HeartbeatWatchdog(redis_client=fake, notifier=notifier, **kw), notifier


@pytest.mark.unit
def test_closes_stall_alerts_once():
    """kline_closes 超过 closes_stall_minutes 无增长 → 告警一次, 不重复。"""
    fake = FakeRedis([make_stats_entry(fresh_iso(), {"kline_closes": 10})])
    watchdog, notifier = _watchdog_with_notifier(fake, closes_stall_minutes=15)
    watchdog.check_once()                                    # 首次观察: 播种基线, 不告警
    assert notifier.send.call_count == 0
    watchdog._last_closes_change_ts -= 16 * 60               # 模拟 16 分钟无增长
    assert watchdog.check_once() == "normal"                 # 主停滞状态不受影响
    assert notifier.send.call_count == 1
    assert "K线闭合" in notifier.send.call_args[0][0]
    assert watchdog.closes_state == "closes_stale"
    watchdog.check_once()                                    # 持续停滞不重复告警
    assert notifier.send.call_count == 1


@pytest.mark.unit
def test_closes_stall_recovers_when_closes_grow():
    """closes 恢复增长 → 恢复通知一次 → normal。"""
    fake = FakeRedis([make_stats_entry(fresh_iso(), {"kline_closes": 10})])
    watchdog, notifier = _watchdog_with_notifier(fake, closes_stall_minutes=15)
    watchdog.check_once()
    watchdog._last_closes_change_ts -= 16 * 60
    watchdog.check_once()
    assert watchdog.closes_state == "closes_stale"
    fake.entries = [make_stats_entry(fresh_iso(), {"kline_closes": 11})]
    watchdog.check_once()
    assert watchdog.closes_state == "recovered"
    assert "恢复" in notifier.send.call_args[0][0]
    watchdog.check_once()
    assert watchdog.closes_state == "normal"
    assert notifier.send.call_count == 2  # 告警 1 + 恢复 1, 无多余


@pytest.mark.unit
def test_fail_rate_alerts_once_until_recovery():
    """订单失败率超阈值 → 告警一次 (连续超阈值不重复)。"""
    fake = FakeRedis([make_stats_entry(fresh_iso(),
                                       {"kline_closes": 1, "orders_placed": 8, "orders_failed": 2})])
    watchdog, notifier = _watchdog_with_notifier(fake, fail_rate_threshold=0.10)
    watchdog.check_once()
    assert notifier.send.call_count == 1
    msg = notifier.send.call_args[0][0]
    assert "失败率" in msg and "20.0%" in msg
    watchdog.check_once()
    watchdog.check_once()
    assert notifier.send.call_count == 1
    assert watchdog.fail_state == "fail_alert"


@pytest.mark.unit
def test_fail_rate_recovers_when_rate_drops():
    """失败率回落到阈值下 → 恢复通知 → normal。"""
    fake = FakeRedis([make_stats_entry(fresh_iso(),
                                       {"kline_closes": 1, "orders_placed": 8, "orders_failed": 2})])
    watchdog, notifier = _watchdog_with_notifier(fake, fail_rate_threshold=0.10)
    watchdog.check_once()
    assert watchdog.fail_state == "fail_alert"
    fake.entries = [make_stats_entry(fresh_iso(),
                                     {"kline_closes": 1, "orders_placed": 100, "orders_failed": 2})]
    watchdog.check_once()
    assert watchdog.fail_state == "recovered"
    assert "恢复" in notifier.send.call_args[0][0]
    watchdog.check_once()
    assert watchdog.fail_state == "normal"


@pytest.mark.unit
def test_normal_stats_no_false_alerts():
    """正常增长 + 低失败率 → 两个新维度均不告警。"""
    fake = FakeRedis([make_stats_entry(fresh_iso(),
                                       {"kline_closes": 5, "orders_placed": 10, "orders_failed": 0})])
    watchdog, notifier = _watchdog_with_notifier(fake)
    for _ in range(5):
        watchdog.check_once()
    fake.entries = [make_stats_entry(fresh_iso(),
                                     {"kline_closes": 6, "orders_placed": 11, "orders_failed": 0})]
    watchdog.check_once()
    assert notifier.send.call_count == 0
    assert watchdog.closes_state == "normal"
    assert watchdog.fail_state == "normal"


@pytest.mark.unit
def test_missing_stats_field_no_alerts():
    """payload 无 stats (旧版 runner) → 新维度静默不误报。"""
    fake = FakeRedis([make_entry(fresh_iso())])
    watchdog, notifier = _watchdog_with_notifier(fake)
    watchdog.check_once()
    watchdog.check_once()
    assert notifier.send.call_count == 0
    assert watchdog.closes_state == "normal"
    assert watchdog.fail_state == "normal"


@pytest.mark.unit
def test_heartbeat_stale_skips_stats_dimensions():
    """心跳停滞时 stats 维度不重复告警 (同一根因, 主停滞告警已覆盖)。"""
    fake = FakeRedis([make_stats_entry(iso_hours_ago(1), {"kline_closes": 10})])
    watchdog, notifier = _watchdog_with_notifier(fake)
    watchdog.check_once()
    assert watchdog._state == "stale"
    assert notifier.send.call_count == 1          # 只有主停滞告警
    assert watchdog.closes_state == "normal"      # closes 维度未触发


@pytest.mark.unit
def test_closes_stall_minutes_parameterized():
    """closes_stall_minutes 参数生效。"""
    fake = FakeRedis([make_stats_entry(fresh_iso(), {"kline_closes": 3})])
    watchdog, notifier = _watchdog_with_notifier(fake, closes_stall_minutes=5)
    watchdog.check_once()
    watchdog._last_closes_change_ts -= 6 * 60     # 6 分钟 > 5 分钟阈值
    watchdog.check_once()
    assert notifier.send.call_count == 1
    assert HeartbeatWatchdog(redis_client=fake, closes_stall_minutes=7).closes_stall_minutes == 7


# ─── Ops T5 补充: DINGTALK_WEBHOOK 旧名兼容 ───


@pytest.mark.unit
def test_build_notifier_falls_back_to_old_webhook_name(monkeypatch):
    """旧名 DINGTALK_WEBHOOK (network_monitor 沿用) 在新名前缺失时兜底。"""
    monkeypatch.delenv("DINGTALK_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("DINGTALK_WEBHOOK", "https://oapi.dingtalk.com/robot/send?access_token=old")
    monkeypatch.setenv("DINGTALK_SECRET", "SECold")
    notifier = build_notifier()
    assert notifier is not None
    assert notifier.webhook_url.startswith("https://oapi.dingtalk.com")
    assert "access_token=old" in notifier.webhook_url
    assert notifier.secret == "SECold"


@pytest.mark.unit
def test_build_notifier_new_name_takes_priority(monkeypatch):
    """两个名字都存在时新名 DINGTALK_WEBHOOK_URL 优先。"""
    monkeypatch.setenv("DINGTALK_WEBHOOK_URL", "https://oapi.dingtalk.com/robot/send?access_token=new")
    monkeypatch.setenv("DINGTALK_WEBHOOK", "https://oapi.dingtalk.com/robot/send?access_token=old")
    notifier = build_notifier()
    assert notifier is not None
    assert "access_token=new" in notifier.webhook_url

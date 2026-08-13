"""tests for tools/preflight_check.py — 全部通过 monkeypatch, 不触网不连 Redis。"""

import json
import time
from datetime import datetime, timedelta, timezone

import pytest

import tools.preflight_check as preflight


class FakeRedis:
    """伪 Redis 客户端: ping 可成功/抛错, xrevrange 返回预设消息。"""

    def __init__(self, ping_result=True, ping_error=None, messages=None):
        self._ping_result = ping_result
        self._ping_error = ping_error
        self._messages = messages or []
        self.ping_calls = 0

    def ping(self):
        self.ping_calls += 1
        if self._ping_error:
            raise self._ping_error
        return self._ping_result

    def xrevrange(self, stream, count=1):
        return self._messages


def _payload_msg(timestamp_iso: str):
    return [("1-0", {"payload": json.dumps({
        "event_id": "e1", "stream": "heartbeat",
        "timestamp": timestamp_iso, "data": {"modules": {}},
    })})]


@pytest.fixture
def fake_redis(monkeypatch):
    """把 preflight._redis_client 换成返回给定 fake client 的工厂。"""
    def _install(client):
        monkeypatch.setattr(preflight, "_redis_client", lambda url: client)
        return client
    return _install


class TestCheckRedis:
    def test_check_redis_ok(self, fake_redis):
        fake_redis(FakeRedis())
        ok, detail = preflight.check_redis()
        assert ok is True
        assert "PONG" in detail
        assert "ms" in detail

    def test_check_redis_fail(self, fake_redis):
        fake_redis(FakeRedis(ping_error=ConnectionError("Connection refused")))
        ok, detail = preflight.check_redis()
        assert ok is False
        assert "ConnectionError" in detail


class TestCheckProxy:
    class _Resp:
        def __init__(self, status_code=200):
            self.status_code = status_code

    def test_check_proxy_latency_ok(self, monkeypatch):
        def fake_get(url, **kwargs):
            time.sleep(0.02)
            return self._Resp(200)
        monkeypatch.setattr(preflight.requests, "get", fake_get)
        ok, detail = preflight.check_proxy(max_latency_ms=1000)
        assert ok is True
        assert "延迟" in detail

    def test_check_proxy_latency_fail(self, monkeypatch):
        def fake_get(url, **kwargs):
            time.sleep(0.2)
            return self._Resp(200)
        monkeypatch.setattr(preflight.requests, "get", fake_get)
        ok, detail = preflight.check_proxy(max_latency_ms=100)
        assert ok is False
        assert "超窗" in detail

    def test_check_proxy_http_error(self, monkeypatch):
        monkeypatch.setattr(preflight.requests, "get",
                            lambda url, **kwargs: self._Resp(502))
        ok, detail = preflight.check_proxy()
        assert ok is False
        assert "HTTP 502" in detail


class TestCheckApiKeys:
    def test_check_api_keys_present(self, monkeypatch, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("BINANCE_API_KEY=testkey123\nBINANCE_API_SECRET=testsecret456\n", encoding="utf-8")
        monkeypatch.delenv("BINANCE_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
        ok, detail = preflight.check_api_keys(str(env_file))
        assert ok is True
        assert "已配置" in detail

    def test_check_api_keys_missing(self, monkeypatch, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("BINANCE_API_KEY=\nBINANCE_API_SECRET=\n", encoding="utf-8")
        monkeypatch.delenv("BINANCE_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
        ok, detail = preflight.check_api_keys(str(env_file))
        assert ok is False
        assert "为空" in detail

    def test_check_api_keys_env_file_absent(self, tmp_path):
        ok, detail = preflight.check_api_keys(str(tmp_path / "nonexistent.env"))
        assert ok is False
        assert "不存在" in detail


class TestCheckServices:
    def test_check_services_all_listening(self, monkeypatch):
        monkeypatch.setattr(preflight, "_port_listening", lambda port: True)
        ok, detail = preflight.check_services()
        assert ok is True
        assert "LISTENING" in detail

    def test_check_services_partial_fail(self, monkeypatch):
        monkeypatch.setattr(preflight, "_port_listening", lambda port: port != 5173)
        ok, detail = preflight.check_services()
        assert ok is False
        assert "5173:CLOSED" in detail


class TestCheckClash:
    def test_clash_port_listening(self, monkeypatch):
        monkeypatch.setattr(preflight, "_port_listening", lambda port: True)
        ok, detail = preflight.check_clash()
        assert ok is True
        assert "LISTENING" in detail

    def test_clash_down_with_process_hint(self, monkeypatch):
        monkeypatch.setattr(preflight, "_port_listening", lambda port: False)
        monkeypatch.setattr(preflight, "_find_clash_process", lambda: "clash-meta.exe")
        ok, detail = preflight.check_clash()
        assert ok is False
        assert "clash-meta.exe" in detail


class TestCheckHeartbeat:
    def test_heartbeat_fresh(self, fake_redis):
        ts = datetime.now(timezone.utc).isoformat()
        fake_redis(FakeRedis(messages=_payload_msg(ts)))
        ok, detail = preflight.check_heartbeat()
        assert ok is True
        assert "心跳" in detail

    def test_heartbeat_stale(self, fake_redis):
        ts = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        fake_redis(FakeRedis(messages=_payload_msg(ts)))
        ok, detail = preflight.check_heartbeat()
        assert ok is False
        assert "超时" in detail or "前" in detail

    def test_heartbeat_empty_stream(self, fake_redis):
        fake_redis(FakeRedis(messages=[]))
        ok, detail = preflight.check_heartbeat()
        assert ok is False
        assert "无消息" in detail

    def test_heartbeat_future_timestamp_fails(self, fake_redis):
        """心跳时间在未来 (时钟漂移) → FAIL, 不因负 age 直接通过。"""
        ts = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        fake_redis(FakeRedis(messages=_payload_msg(ts)))
        ok, detail = preflight.check_heartbeat()
        assert ok is False
        assert "时钟" in detail


class TestSummarize:
    def test_summary_exit_code_all_pass(self):
        results = [("Redis", True, "PONG"), ("代理", True, "延迟 80ms")]
        all_pass, code = preflight.summarize(results)
        assert all_pass is True
        assert code == 0

    def test_summary_exit_code_any_fail(self):
        results = [("Redis", True, "PONG"), ("代理", False, "延迟 7500ms 超窗")]
        all_pass, code = preflight.summarize(results)
        assert all_pass is False
        assert code == 1

    def test_summary_exit_code_multiple_fail(self):
        results = [("Redis", False, "x"), ("代理", False, "y")]
        all_pass, code = preflight.summarize(results)
        assert all_pass is False
        assert code == 1

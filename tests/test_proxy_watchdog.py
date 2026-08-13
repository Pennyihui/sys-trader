"""proxy_watchdog 测试：探测判定 + 状态机 + 切换/告警（mock requests/notifier）。

不碰真实网络：requests.get 全部 patch；apply_config 用 sys.modules 假模块
注入，不加载 tools/proxy_pool 真实代码。
"""
import json
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests

from tools import proxy_watchdog as pw
from tools.proxy_watchdog import ProxyWatchdog


class FakeResponse:
    status_code = 200


# ---------- 探测判定 ----------

class TestProbe:
    def test_probe_success_returns_latency_ms(self):
        with patch.object(pw.requests, "get", return_value=FakeResponse()) as mock_get:
            lat = pw._probe_latency_ms()
        assert isinstance(lat, float) and lat >= 0.0
        # 走了 7897 代理 + 12s 超时
        assert mock_get.call_args[0][0] == pw.PROBE_URL
        assert mock_get.call_args[1]["proxies"]["https"] == pw.PROXY_URL
        assert mock_get.call_args[1]["timeout"] == 12.0

    def test_probe_non_200_is_failure(self):
        resp = FakeResponse()
        resp.status_code = 502
        with patch.object(pw.requests, "get", return_value=resp):
            assert pw._probe_latency_ms() is None

    def test_probe_connection_error_is_failure(self):
        with patch.object(pw.requests, "get",
                          side_effect=requests.ConnectionError("Connection refused")):
            assert pw._probe_latency_ms() is None

    def test_probe_timeout_is_failure(self):
        with patch.object(pw.requests, "get", side_effect=requests.Timeout("timed out")):
            assert pw._probe_latency_ms(timeout=12.0) is None


# ---------- 状态机 ----------

@pytest.fixture
def wd():
    """阈值 5000ms / 连续 3 次 / 冷却 300s，notifier 为 mock。"""
    w = ProxyWatchdog(threshold_ms=5000, consecutive=3, cooldown=300,
                      notifier=MagicMock())
    w.bad_streak = 0
    w._last_action_ts = 0.0
    return w


class TestStateMachine:
    def test_good_probe_resets_streak(self, wd):
        wd.bad_streak = 2
        with patch.object(wd, "probe", return_value=200.0):
            result = wd.check_once()
        assert result["slow"] is False
        assert result["bad_streak"] == 0
        assert result["triggered"] is False

    def test_slow_probe_increments_streak(self, wd):
        with patch.object(wd, "probe", return_value=6000.0):
            result = wd.check_once()
        assert result["slow"] is True
        assert result["bad_streak"] == 1
        assert result["triggered"] is False

    def test_failed_probe_counts_as_bad(self, wd):
        with patch.object(wd, "probe", return_value=None):
            result = wd.check_once()
        assert result["slow"] is True
        assert result["bad_streak"] == 1
        assert result["latency_ms"] is None

    def test_consecutive_slow_triggers_failover(self, wd):
        with patch.object(wd, "probe", return_value=6000.0), \
             patch.object(wd, "switch_node", return_value=True) as mock_switch:
            for _ in range(2):
                wd.check_once()
            result = wd.check_once()
        assert result["triggered"] is True
        assert result["switched"] is True
        assert result["notified"] is True
        mock_switch.assert_called_once()
        assert result["bad_streak"] >= wd.consecutive

    def test_cooldown_blocks_second_trigger(self, wd):
        """cooldown 内即使再次连续超标也不重复切换/告警。"""
        import time
        wd._last_action_ts = time.time()  # 假装刚动作过
        with patch.object(wd, "probe", return_value=6000.0), \
             patch.object(wd, "switch_node", return_value=True) as mock_switch:
            wd.bad_streak = 0
            for _ in range(3):
                wd.check_once()
        assert mock_switch.call_count == 0
        assert wd.notifier.send.call_count == 0

    def test_cooldown_expired_allows_new_trigger(self, wd):
        import time
        wd._last_action_ts = time.time() - wd.cooldown - 1  # 冷却已过
        with patch.object(wd, "probe", return_value=6000.0), \
             patch.object(wd, "switch_node", return_value=True) as mock_switch:
            wd.bad_streak = 0
            for _ in range(3):
                wd.check_once()
        assert mock_switch.call_count == 1

    def test_recovery_after_failover(self, wd):
        """切换后探测恢复正常 → streak 清零，不再动作。"""
        with patch.object(wd, "probe", return_value=6000.0), \
             patch.object(wd, "switch_node", return_value=True):
            for _ in range(3):
                wd.check_once()
        assert wd.bad_streak >= 3
        with patch.object(wd, "probe", return_value=200.0), \
             patch.object(wd, "switch_node") as mock_switch:
            result = wd.check_once()
        assert result["bad_streak"] == 0
        assert result["triggered"] is False
        mock_switch.assert_not_called()


# ---------- 切换 ----------

class TestSwitchNode:
    def test_switch_node_loads_pool_and_calls_apply_config(self, tmp_path):
        pool = {"last_updated": "", "proxies": [{"name": "n1", "healthy": True}]}
        (tmp_path / "proxy_pool.json").write_text(json.dumps(pool), encoding="utf-8")
        fake = MagicMock()
        fake.apply_config.return_value = True
        with patch.dict(sys.modules, {"clash_updater": fake}):
            w = ProxyWatchdog(pool_dir=str(tmp_path), notifier=MagicMock())
            assert w.switch_node() is True
        fake.apply_config.assert_called_once()
        args, kwargs = fake.apply_config.call_args
        assert args[0]["proxies"][0]["name"] == "n1"
        assert kwargs.get("force_reload") is True

    def test_switch_node_apply_failure_returns_false(self, tmp_path):
        (tmp_path / "proxy_pool.json").write_text(json.dumps({"proxies": []}),
                                                  encoding="utf-8")
        fake = MagicMock()
        fake.apply_config.return_value = False
        with patch.dict(sys.modules, {"clash_updater": fake}):
            w = ProxyWatchdog(pool_dir=str(tmp_path), notifier=MagicMock())
            assert w.switch_node() is False

    def test_switch_node_missing_pool_returns_false(self, tmp_path):
        w = ProxyWatchdog(pool_dir=str(tmp_path), notifier=MagicMock())
        assert w.switch_node() is False

    def test_switch_node_bad_json_returns_false(self, tmp_path):
        (tmp_path / "proxy_pool.json").write_text("{not json", encoding="utf-8")
        w = ProxyWatchdog(pool_dir=str(tmp_path), notifier=MagicMock())
        assert w.switch_node() is False

    def test_switch_node_import_error_returns_false(self, tmp_path):
        (tmp_path / "proxy_pool.json").write_text(json.dumps({"proxies": []}),
                                                  encoding="utf-8")
        with patch.dict(sys.modules, {"clash_updater": None}):  # import 失败
            w = ProxyWatchdog(pool_dir=str(tmp_path), notifier=MagicMock())
            assert w.switch_node() is False


# ---------- 告警 ----------

class TestNotify:
    def test_notify_sends_message_with_context(self, wd):
        result = {"latency_ms": 6000.0, "bad_streak": 3}
        wd.bad_streak = 3
        wd.notifier.send.return_value = True
        assert wd.notify(result, switched=True) is True
        msg = wd.notifier.send.call_args[0][0]
        assert "6000" in msg
        assert "切换" in msg
        assert "成功" in msg

    def test_notify_reports_switch_failure(self, wd):
        result = {"latency_ms": None}
        wd.notifier.send.return_value = True
        wd.notify(result, switched=False)
        msg = wd.notifier.send.call_args[0][0]
        assert "失败（请手动处理）" in msg

    def test_notify_without_notifier_degrades_gracefully(self, wd):
        wd.notifier = None
        assert wd.notify({"latency_ms": 6000.0}, switched=True) is False

    def test_notify_send_exception_returns_false(self, wd):
        wd.notifier.send.side_effect = Exception("boom")
        assert wd.notify({"latency_ms": 6000.0}, switched=True) is False


# ---------- 通知器构建 ----------

class TestMakeNotifier:
    def test_no_env_returns_none(self):
        with patch.dict("os.environ", {}, clear=False):
            with patch.dict("os.environ", {
                "DINGTALK_WEBHOOK_URL": "",
                "DINGTALK_WEBHOOK": "",
            }):
                from tools.proxy_watchdog import make_notifier
                assert make_notifier() is None

    def test_with_env_returns_notifier(self):
        with patch.dict("os.environ",
                        {"DINGTALK_WEBHOOK_URL": "https://oapi.dingtalk.com/robot/send?x=1"},
                        clear=False):
            from tools.proxy_watchdog import make_notifier
            n = make_notifier()
        assert n is not None
        assert n.webhook_url.startswith("https://oapi.dingtalk.com")

"""测试钉钉通知器。"""
import pytest
from unittest.mock import MagicMock, patch
from monitor.dingtalk import DingTalkNotifier
from monitor.alerter import Alert, AlertLevel


class TestDingTalkNotifier:
    def setup_method(self):
        self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=test"
        self.notifier = DingTalkNotifier(self.webhook)

    @patch("monitor.dingtalk.requests.post")
    def test_send_text_success(self, mock_post):
        mock_post.return_value.json.return_value = {"errcode": 0}
        mock_post.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"errcode": 0}
        ok = self.notifier.send("测试消息")
        assert ok is True
        # 验证请求体: 消息统一带 [SysTrader] 关键词前缀 (钉钉自定义关键词大小写敏感)
        args, kwargs = mock_post.call_args
        assert args[0] == self.webhook
        assert kwargs["json"]["msgtype"] == "text"
        assert kwargs["json"]["text"]["content"] == "[SysTrader] 测试消息"

    @patch("monitor.dingtalk.requests.post")
    def test_send_text_prefix_not_duplicated(self, mock_post):
        """已含 [SysTrader] 前缀的消息 (如 heartbeat_watchdog._dispatch 侧已加) 不重复加。"""
        mock_post.return_value.json.return_value = {"errcode": 0}
        mock_post.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"errcode": 0}
        self.notifier.send("[SysTrader] 已有前缀")
        _args, kwargs = mock_post.call_args
        assert kwargs["json"]["text"]["content"] == "[SysTrader] 已有前缀"

    @patch("monitor.dingtalk.requests.post")
    def test_send_text_api_error(self, mock_post):
        mock_post.return_value.json.return_value = {"errcode": 310000, "errmsg": "keywords not in content"}
        ok = self.notifier.send("无关键词")
        assert ok is False

    @patch("monitor.dingtalk.requests.post")
    def test_send_text_network_error(self, mock_post):
        mock_post.side_effect = Exception("Connection refused")
        ok = self.notifier.send("测试")
        assert ok is False

    def test_send_alert_info_filtered_by_default(self):
        """默认 min_level=WARNING，INFO 不应发送"""
        notifier = DingTalkNotifier(self.webhook)  # 默认 WARNING
        alert = Alert(level=AlertLevel.INFO, metric="signal", message="info")
        with patch.object(notifier, "send") as mock_send:
            sent = notifier.send_alert(alert)
            assert sent is False
            mock_send.assert_not_called()

    @patch("monitor.dingtalk.requests.post")
    def test_send_alert_critical_sends(self, mock_post):
        mock_post.return_value.json.return_value = {"errcode": 0}
        alert = Alert(level=AlertLevel.CRITICAL, metric="margin_ratio",
                      message="Margin 85%", context={"ratio": 0.85})
        sent = self.notifier.send_alert(alert)
        assert sent is True
        args, kwargs = mock_post.call_args
        content = kwargs["json"]["text"]["content"]
        assert "CRITICAL" in content
        assert "margin_ratio" in content

    def test_markdown_message(self):
        notifier = DingTalkNotifier(self.webhook, min_level=AlertLevel.INFO)
        with patch.object(notifier, "_post") as mock_post:
            mock_post.return_value = True
            ok = notifier.send_markdown("标题", "**加粗**内容")
            assert ok is True
            payload = mock_post.call_args[0][0]
            assert payload["msgtype"] == "markdown"
            assert payload["markdown"]["title"] == "标题"

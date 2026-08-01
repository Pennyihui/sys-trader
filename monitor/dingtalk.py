"""钉钉机器人通知 — 通过 Webhook 推送告警消息。

支持两种安全设置：
  - 自定义关键词：直接使用 Webhook
  - 加签 (Secret)：自动计算 HMAC-SHA256 签名

用法:
    from monitor.dingtalk import DingTalkNotifier
    from monitor.alerter import Alerter, AlertLevel

    notifier = DingTalkNotifier(webhook_url, secret="SECxxx")
    alerter = Alerter(on_alert=notifier.send_alert)
"""

import base64
import hashlib
import hmac
import logging
import time
import json
import urllib.parse
from typing import Optional

import requests

from monitor.alerter import Alert, AlertLevel

logger = logging.getLogger(__name__)


class DingTalkNotifier:
    """钉钉自定义机器人通知器。"""

    def __init__(self, webhook_url: str, secret: str = "",
                 min_level: AlertLevel = AlertLevel.WARNING):
        self.webhook_url = webhook_url
        self.secret = secret
        self.min_level = min_level
        self._level_rank = {AlertLevel.INFO: 0, AlertLevel.WARNING: 1, AlertLevel.CRITICAL: 2}

    def _signed_url(self) -> str:
        """如果配置了 secret，生成带签名的完整 URL。"""
        if not self.secret:
            return self.webhook_url
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            self.secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        sep = "&" if "?" in self.webhook_url else "?"
        return f"{self.webhook_url}{sep}timestamp={timestamp}&sign={sign}"

    def send(self, message: str) -> bool:
        """发送纯文本消息。"""
        payload = {
            "msgtype": "text",
            "text": {"content": message},
        }
        return self._post(payload)

    def send_markdown(self, title: str, text: str) -> bool:
        """发送 markdown 消息。"""
        payload = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": text},
        }
        return self._post(payload)

    def send_alert(self, alert: Alert) -> bool:
        """作为 Alerter 的 on_alert 回调使用。"""
        if self._level_rank.get(alert.level, 0) < self._level_rank[self.min_level]:
            return False
        icon = {"INFO": "ℹ️", "WARNING": "⚠️", "CRITICAL": "🚨"}.get(alert.level.value, "")
        msg = f"{icon} [{alert.level.value}] {alert.metric}\n{alert.message}"
        if alert.context:
            msg += f"\n{json.dumps(alert.context, ensure_ascii=False)}"
        return self.send(msg)

    def _post(self, payload: dict) -> bool:
        try:
            resp = requests.post(self._signed_url(), json=payload, timeout=10)
            data = resp.json()
            if data.get("errcode") == 0:
                logger.info("DingTalk message sent")
                return True
            logger.error("DingTalk API error: %s", data)
            return False
        except Exception as e:
            logger.error("DingTalk request failed: %s", e)
            return False

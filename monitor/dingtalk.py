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
import os
import time
import json
import urllib.parse
from typing import Optional

import requests

from monitor.alerter import Alert, AlertLevel

logger = logging.getLogger(__name__)

# 钉钉自定义机器人关键词（大小写敏感）: 消息不含该词会被 API 拒绝 (310000)
_KEYWORD = "[SysTrader]"


class DingTalkNotifier:
    """钉钉自定义机器人通知器。"""

    def __init__(self, webhook_url: str, secret: str = "",
                 min_level: AlertLevel = AlertLevel.WARNING):
        self.webhook_url = webhook_url
        self.secret = secret
        self.min_level = min_level
        self._level_rank = {AlertLevel.INFO: 0, AlertLevel.WARNING: 1, AlertLevel.CRITICAL: 2}
        # 告警历史归档 (2026-08-16 面板二期): 每次发送把消息同步进 Redis
        # "alert" 流 (best-effort), 运维面板展示告警时间线
        self._redis = None
        redis_url = os.environ.get("REDIS_URL", "")
        if redis_url:
            try:
                import redis
                # socket_connect_timeout: 告警路径不能被宕机 Redis 阻塞 (2026-08-16 审计)
                self._redis = redis.Redis.from_url(
                    redis_url, decode_responses=True,
                    socket_connect_timeout=2, socket_timeout=2)
            except Exception:
                self._redis = None

    def _publish_alert(self, message: str, ok: bool):
        if self._redis is None:
            return
        try:
            import uuid
            from datetime import datetime, timezone
            payload = json.dumps({
                "event_id": str(uuid.uuid4()),
                "stream": "alert",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {"source": "dingtalk", "message": message,
                         "delivered": ok},
            }, ensure_ascii=False)
            self._redis.xadd("systrader:alert", {"payload": payload}, maxlen=5000)
        except Exception:
            pass  # 归档是观测增强, 失败不阻塞告警主链路

    def _prefixed(self, message: str) -> str:
        """统一给消息加 [SysTrader] 关键词前缀（大小写敏感）。

        调用侧可能已加前缀（如 heartbeat_watchdog._dispatch），已含则不重复添加。
        """
        return message if _KEYWORD in message else f"{_KEYWORD} {message}"

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
        """发送纯文本消息（统一加 [SysTrader] 关键词前缀）。"""
        payload = {
            "msgtype": "text",
            "text": {"content": self._prefixed(message)},
        }
        return self._post(payload)

    def send_at(self, message: str, mobiles=None) -> bool:
        """发送文本消息并 @指定手机号 (2026-08-16 风控补强 #7)。

        CRITICAL 级告警 (熔断/减仓/强平边缘) 用 @ 拉人。atMobiles 为空时
        退化为普通 send。加签模式下 at 字段与签名无冲突, 安全设置只需关键词。
        """
        mobiles = [m for m in (mobiles or []) if m]
        if not mobiles:
            return self.send(message)
        payload = {
            "msgtype": "text",
            "text": {"content": self._prefixed(message)},
            "at": {"atMobiles": mobiles, "isAtAll": False},
        }
        return self._post(payload)

    def send_markdown(self, title: str, text: str) -> bool:
        """发送 markdown 消息。

        钉钉关键词安全模式下 markdown 消息同样校验关键词 (title 或 text
        至少一处含 [SysTrader]), 原实现漏加会被 310000 拒绝 (2026-08-16 审计)。
        """
        payload = {
            "msgtype": "markdown",
            "markdown": {"title": self._prefixed(title), "text": text},
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
                self._publish_alert(payload.get("text", {}).get("content", "") or
                                    payload.get("markdown", {}).get("title", ""), True)
                return True
            logger.error("DingTalk API error: %s", data)
            self._publish_alert(str(data)[:300], False)
            return False
        except Exception as e:
            logger.error("DingTalk request failed: %s", e)
            self._publish_alert(f"DingTalk 请求失败: {e}"[:300], False)
            return False

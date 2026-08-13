"""告警模块 — 网络状态变化时推送钉钉通知。

状态机: 正常 -> 故障 -> 恢复
  - 正常 -> 故障: 推送 CRITICAL 告警
  - 故障 -> 恢复: 推送 INFO 恢复通知
  - 连续故障: 只推送一次（避免告警风暴），每隔 N 分钟提醒一次
"""

import logging
import os
import sys
import time
from typing import Optional

logger = logging.getLogger(__name__)

# 导入项目内的 dingtalk 通知器
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

WEBHOOK_URL = os.environ.get("DINGTALK_WEBHOOK", "")
WEBHOOK_SECRET = os.environ.get("DINGTALK_SECRET", "")

# 告警抑制: 同一状态变化 5 分钟内不重复推送
ALERT_COOLDOWN = 300


class NetworkNotifier:
    def __init__(self, webhook_url: str = "", secret: str = ""):
        self.webhook_url = webhook_url or WEBHOOK_URL
        self.secret = secret or WEBHOOK_SECRET
        self._enabled = bool(self.webhook_url)
        self._last_state: Optional[bool] = None  # True=正常 False=故障
        self._last_alert_ts = 0.0
        self._last_alert_state: Optional[bool] = None

        if self._enabled:
            logger.info("钉钉告警已启用")
        else:
            logger.warning("钉钉告警未配置 (设置 DINGTALK_WEBHOOK 环境变量)")

    def update(self, network_ok: bool, detail: dict) -> Optional[dict]:
        """状态变化时推送告警。返回推送的告警信息，未推送返回 None。"""
        if not self._enabled:
            self._last_state = network_ok
            return None

        now = time.time()
        state_changed = (self._last_state is not None
                         and network_ok != self._last_state)

        # 状态变化或首次检查
        if state_changed or self._last_state is None:
            level = "INFO" if network_ok else "CRITICAL"
            title = "网络恢复" if network_ok else "网络故障"
            msg = self._format_message(title, detail)
            self._push(level, msg)
            self._last_state = network_ok
            self._last_alert_ts = now
            self._last_alert_state = network_ok
            return {"level": level, "message": msg}

        # 持续故障: 每 5 分钟提醒一次
        if not network_ok and now - self._last_alert_ts > ALERT_COOLDOWN:
            msg = self._format_message("网络持续故障", detail)
            self._push("CRITICAL", msg)
            self._last_alert_ts = now
            return {"level": "CRITICAL", "message": msg}

        self._last_state = network_ok
        return None

    def _format_message(self, title: str, detail: dict) -> str:
        return (
            f"【{title}】\n"
            f"网关: {detail.get('gateway', '?')} "
            f"{'✅' if detail.get('gateway_ok') else '❌'} "
            f"({detail.get('gateway_ms', '-')}ms)\n"
            f"DNS(223.5.5.5): "
            f"{'✅' if detail.get('dns_ok') else '❌'} "
            f"({detail.get('dns_ms', '-')}ms)\n"
            f"Clash(7897): "
            f"{'✅' if detail.get('clash_ok') else '❌'}\n"
            f"代理池(8765): "
            f"{'✅' if detail.get('pool_ok') else '❌'}\n"
            f"时间: {time.strftime('%m-%d %H:%M:%S', time.localtime(detail.get('timestamp', time.time())))}"
        )

    def _push(self, level: str, text: str):
        try:
            from monitor.dingtalk import DingTalkNotifier
            notifier = DingTalkNotifier(self.webhook_url, secret=self.secret)
            # send_alert 签名是 send_alert(alert: Alert)，不是 (level, message)——
            # 直接 send 文本；[SysTrader] 前缀对应钉钉自定义关键词（大小写敏感），
            # 缺失会被 310000 拒绝，send 内部不会自动加，这里显式带上
            ok = notifier.send(f"[SysTrader][network-monitor] {text}")
            if ok:
                logger.info("钉钉告警已推送 [%s]", level)
            else:
                logger.error("钉钉推送被拒 [%s]", level)
        except Exception as e:
            logger.error("钉钉推送失败: %s", e)


def create_notifier() -> NetworkNotifier:
    return NetworkNotifier()
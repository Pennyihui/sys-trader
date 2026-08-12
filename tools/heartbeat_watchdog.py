"""heartbeat_watchdog — 监控 heartbeat 事件流，检测主系统停滞并钉钉告警。

背景: 后台进程可能静默挂起（进程活着但主循环停摆、无日志）。本工具订阅
heartbeat 事件流（EventBus 发布到 Redis Stream `systrader:heartbeat`），
若超过 stale_after 秒无新事件 → 钉钉告警。

用法: python tools/heartbeat_watchdog.py [--stale-after 60] [--interval 10] [--redis-url ...]

环境变量:
  REDIS_URL             Redis 连接串 (默认 redis://localhost:6379)
  DINGTALK_WEBHOOK_URL  钉钉自定义机器人 webhook；未配置时告警降级为 logger.warning
  DINGTALK_SECRET       钉钉加签 secret (可选)
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis

from shared.config_loader import load_env

logger = logging.getLogger(__name__)

HEARTBEAT_STREAM = "systrader:heartbeat"


class HeartbeatWatchdog:
    """轮询 heartbeat 流最后一条事件，检测停滞并触发告警。

    状态机: normal → stale(告警一次) → 持续 stale 不重复告警 →
    recovered(可选恢复通知) → normal。重新停滞时再次告警。

    Redis 可注入（redis_client 参数，或直接替换 .redis）便于测试；
    notifier 为告警回调：DingTalkNotifier（有 .send）或纯函数，
    None 时降级为 logger.warning（webhook 未配置时不崩溃）。
    """

    def __init__(self, redis_url: str = "redis://localhost:6379",
                 stale_after: float = 60.0, interval: float = 10.0,
                 notifier=None, redis_client: Any = None,
                 stream_key: str = HEARTBEAT_STREAM,
                 notify_recovery: bool = True):
        self.redis_url = redis_url
        self.stale_after = stale_after
        self.interval = interval
        self.notifier = notifier
        self.stream_key = stream_key
        self.notify_recovery = notify_recovery
        self.redis = redis_client if redis_client is not None \
            else redis.Redis.from_url(redis_url, decode_responses=True)
        self._state = "normal"

    # ── 读取 ──

    @staticmethod
    def _decode(v: Any) -> str:
        """xrevrange 的 fields 值可能是 bytes（decode_responses=False 时），双保险 decode。"""
        return v.decode("utf-8", errors="replace") if isinstance(v, bytes) else v

    def last_event_age(self) -> float:
        """读取 heartbeat 流最后一条消息的 payload.timestamp，返回距现在的秒数。

        流为空 / payload 缺失 / 时间戳不可解析 → 返回 float("inf")（fail-safe: 视为停滞）。
        Redis 不可达时抛出异常，由调用方（run_forever）捕获，watchdog 不崩溃。
        """
        entries = self.redis.xrevrange(self.stream_key, count=1)
        if not entries:
            return float("inf")
        fields = entries[0][1]
        payload_raw = self._decode(fields.get("payload", ""))
        try:
            payload = json.loads(payload_raw)
        except (ValueError, TypeError):
            return float("inf")
        ts_raw = self._decode(payload.get("timestamp", ""))
        try:
            ts = datetime.fromisoformat(ts_raw)
        except (ValueError, TypeError):
            return float("inf")
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)  # 无时区字段按 UTC 处理
        return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())

    # ── 状态机 ──

    def check_once(self) -> str:
        """单轮检查，返回当前状态（normal / stale / recovered），便于测试断言。"""
        age = self.last_event_age()
        if age > self.stale_after:
            if self._state != "stale":  # normal→stale 或 recovered→stale 都告警一次
                self._dispatch(self._stale_message(age))
            self._state = "stale"
        else:
            if self._state == "stale":
                if self.notify_recovery:
                    self._dispatch(self._recovery_message(age))
                self._state = "recovered"
            elif self._state == "recovered":
                self._state = "normal"
        logger.info("watchdog state=%s age=%.1fs (stale_after=%.0fs)",
                    self._state, age, self.stale_after)
        return self._state

    def run_forever(self):
        """轮询循环：单轮异常（如 Redis 抖动）只记日志，watchdog 不退出。"""
        while True:
            try:
                self.check_once()
            except Exception as e:
                logger.error("watchdog 检查失败: %s", e)
            time.sleep(self.interval)

    # ── 告警 ──

    def _dispatch(self, message: str) -> bool:
        """发送告警；notifier 为 None 时降级为 logger.warning（不崩溃）。"""
        if self.notifier is None:
            logger.warning("HEARTBEAT WATCHDOG: %s", message)
            return False
        if hasattr(self.notifier, "send"):
            return bool(self.notifier.send(message))
        return bool(self.notifier(message))

    def _stale_message(self, age: float) -> str:
        return (f"[WATCHDOG] 心跳停滞告警: 主系统 heartbeat 事件已 {age:.0f}s 无更新"
                f"（阈值 {self.stale_after:.0f}s），进程可能已静默挂起，请立即检查！")

    def _recovery_message(self, age: float) -> str:
        return f"[WATCHDOG] 心跳恢复: heartbeat 事件已恢复（age={age:.1f}s）。"


def build_notifier() -> Optional[Any]:
    """根据环境变量构造钉钉通知器；DINGTALK_WEBHOOK_URL 未配置时返回 None（降级日志）。"""
    webhook = os.environ.get("DINGTALK_WEBHOOK_URL", "").strip()
    if not webhook:
        logger.warning("DINGTALK_WEBHOOK_URL 未配置 — 告警降级为本地日志")
        return None
    from monitor.dingtalk import DingTalkNotifier  # 延迟导入，本模块不依赖钉钉也能跑
    return DingTalkNotifier(webhook, secret=os.environ.get("DINGTALK_SECRET", ""))


def main():
    load_env()
    parser = argparse.ArgumentParser(description="心跳停滞检测 — 监控 heartbeat 事件流，停滞时钉钉告警")
    parser.add_argument("--stale-after", type=float, default=60.0,
                        help="无新 heartbeat 事件超过该秒数判定为停滞 (默认 60)")
    parser.add_argument("--interval", type=float, default=10.0,
                        help="轮询间隔秒数 (默认 10)")
    parser.add_argument("--redis-url", type=str, default="",
                        help="Redis 连接串 (默认取 REDIS_URL env，再默认 redis://localhost:6379)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    notifier = build_notifier()
    logger.info("钉钉告警已启用" if notifier is not None else "钉钉未配置，告警降级为日志")

    redis_url = args.redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379")
    watchdog = HeartbeatWatchdog(redis_url, stale_after=args.stale_after,
                                 interval=args.interval, notifier=notifier)
    logger.info("heartbeat_watchdog 启动: redis=%s stale_after=%.0fs interval=%.0fs",
                redis_url, args.stale_after, args.interval)
    watchdog.run_forever()


if __name__ == "__main__":
    main()

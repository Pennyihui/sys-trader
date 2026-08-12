"""heartbeat_watchdog — 监控 heartbeat 事件流，检测主系统停滞并钉钉告警。

背景: 后台进程可能静默挂起（进程活着但主循环停摆、无日志）。本工具订阅
heartbeat 事件流（EventBus 发布到 Redis Stream `systrader:heartbeat`），
若超过 stale_after 秒无新事件 → 钉钉告警。

Ops T5 扩展: 解析 heartbeat payload 的 stats 字段 (runner 发布的 gauges)，
新增两个独立检测维度:
  - K线闭合停滞: kline_closes 超过 closes_stall_minutes 分钟无增长 → 告警
  - 订单失败率:   orders_failed / (orders_placed + orders_failed)
                  超过 fail_rate_threshold → 告警

用法: python tools/heartbeat_watchdog.py [--stale-after 60] [--interval 10]
      [--redis-url ...] [--closes-stall-minutes 15] [--fail-rate-threshold 0.10]

环境变量:
  REDIS_URL             Redis 连接串 (默认 redis://localhost:6379)
  DINGTALK_WEBHOOK_URL  钉钉自定义机器人 webhook (优先)；未配置时告警降级为 logger.warning
  DINGTALK_WEBHOOK      旧名 webhook 变量 (network_monitor 沿用, 兜底)
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
                 notify_recovery: bool = True,
                 closes_stall_minutes: float = 15.0,
                 fail_rate_threshold: float = 0.10):
        self.redis_url = redis_url
        self.stale_after = stale_after
        self.interval = interval
        self.notifier = notifier
        self.stream_key = stream_key
        self.notify_recovery = notify_recovery
        self.redis = redis_client if redis_client is not None \
            else redis.Redis.from_url(redis_url, decode_responses=True)
        self._state = "normal"
        # Ops T5: stats 维度独立状态机
        self.closes_state = "normal"      # normal / closes_stale / recovered
        self.fail_state = "normal"        # normal / fail_alert / recovered
        self.closes_stall_minutes = closes_stall_minutes
        self.fail_rate_threshold = fail_rate_threshold
        self._last_closes_value: Optional[float] = None
        self._last_closes_change_ts: float = 0.0

    # ── 读取 ──

    @staticmethod
    def _decode(v: Any) -> str:
        """xrevrange 的 fields 值可能是 bytes（decode_responses=False 时），双保险 decode。"""
        return v.decode("utf-8", errors="replace") if isinstance(v, bytes) else v

    def _last_event_payload(self) -> Optional[dict]:
        """读取 heartbeat 流最后一条消息的 payload（EventBus envelope dict）。

        流为空 / payload 缺失 / 非 JSON → None（fail-safe）。
        Redis 不可达时抛出异常，由调用方（run_forever）捕获，watchdog 不崩溃。
        """
        entries = self.redis.xrevrange(self.stream_key, count=1)
        if not entries:
            return None
        payload_raw = self._decode(entries[0][1].get("payload", ""))
        try:
            return json.loads(payload_raw)
        except (ValueError, TypeError):
            return None

    def last_event_age(self) -> float:
        """读取 heartbeat 流最后一条消息的 payload.timestamp，返回距现在的秒数。

        流为空 / payload 缺失 / 时间戳不可解析 → 返回 float("inf")（fail-safe: 视为停滞）。
        """
        payload = self._last_event_payload()
        if not payload:
            return float("inf")
        ts_raw = self._decode(payload.get("timestamp", ""))
        try:
            ts = datetime.fromisoformat(ts_raw)
        except (ValueError, TypeError):
            return float("inf")
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)  # 无时区字段按 UTC 处理
        return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())

    def last_event_stats(self) -> Optional[dict]:
        """读取最后一条 heartbeat 消息的 stats 字段（runner 发布的 gauges 快照）。

        EventBus envelope 中 payload 与 data 是两层: 真实消息 stats 在 data.stats,
        同时兼容顶层 stats (测试/手工构造)。缺失/非 dict → None（该维度不评估）。
        """
        payload = self._last_event_payload()
        if not payload:
            return None
        stats = payload.get("stats")
        if not isinstance(stats, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                stats = data.get("stats")
        return stats if isinstance(stats, dict) else None

    # ── 状态机 ──

    def check_once(self) -> str:
        """单轮检查，返回主状态（normal / stale / recovered），便于测试断言。

        stats 维度 (closes_state / fail_state) 为独立状态机，随主循环一起推进；
        心跳停滞时不评估 stats 维度（同一根因，避免重复告警）。
        """
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
            stats = self.last_event_stats()
            self._check_closes_stall(stats)
            self._check_fail_rate(stats)
        logger.info("watchdog state=%s closes_state=%s fail_state=%s age=%.1fs (stale_after=%.0fs)",
                    self._state, self.closes_state, self.fail_state, age, self.stale_after)
        return self._state

    def run_forever(self):
        """轮询循环：单轮异常（如 Redis 抖动）只记日志，watchdog 不退出。"""
        while True:
            try:
                self.check_once()
            except Exception as e:
                logger.error("watchdog 检查失败: %s", e)
            time.sleep(self.interval)

    # ── Ops T5: stats 维度检测 (K线闭合停滞 / 订单失败率) ──

    def _check_closes_stall(self, stats: Optional[dict]):
        """kline_closes 超过 closes_stall_minutes 无增长 → 告警（状态机, 告警一次）。

        首次观察只播种基线不告警；恢复增长 → 恢复通知一次 → normal。
        stats 缺失（旧版 runner 未发布）→ 该维度不评估, 不误报。
        """
        if not stats or not isinstance(stats, dict):
            return
        closes = stats.get("kline_closes")
        if not isinstance(closes, (int, float)):
            return
        now = time.time()
        if self._last_closes_value is None or closes > self._last_closes_value:
            self._last_closes_value = closes
            self._last_closes_change_ts = now
            if self.closes_state == "closes_stale":
                if self.notify_recovery:
                    self._dispatch(self._closes_recovery_message())
                self.closes_state = "recovered"
            else:
                self.closes_state = "normal"
        elif now - self._last_closes_change_ts > self.closes_stall_minutes * 60:
            if self.closes_state != "closes_stale":
                self._dispatch(
                    self._closes_stale_message(now - self._last_closes_change_ts))
            self.closes_state = "closes_stale"
        else:
            self.closes_state = "normal"  # 仍在增长周期内

    def _check_fail_rate(self, stats: Optional[dict]):
        """订单失败率 = orders_failed / (orders_placed + orders_failed) 超阈值 → 告警。

        状态机: 超阈值才告警一次, 回落阈值下 → 恢复通知一次 → normal。
        """
        if not stats or not isinstance(stats, dict):
            return
        placed = stats.get("orders_placed", 0)
        failed = stats.get("orders_failed", 0)
        if not isinstance(placed, (int, float)) or not isinstance(failed, (int, float)):
            return
        total = placed + failed
        rate = failed / total if total > 0 else 0.0
        if rate > self.fail_rate_threshold:
            if self.fail_state != "fail_alert":
                self._dispatch(self._fail_rate_message(rate, failed, placed))
            self.fail_state = "fail_alert"
        else:
            if self.fail_state == "fail_alert":
                if self.notify_recovery:
                    self._dispatch(self._fail_rate_recovery_message(rate))
                self.fail_state = "recovered"
            elif self.fail_state == "recovered":
                self.fail_state = "normal"

    # ── 告警 ──

    def _dispatch(self, message: str) -> bool:
        """发送告警；notifier 为 None 时降级为 logger.warning（不崩溃）。

        消息统一加 [SysTrader] 前缀：钉钉机器人设置了自定义关键词
        "SysTrader"（大小写敏感），不带前缀的消息会被 API 拒绝 (310000)。
        """
        message = f"[SysTrader] {message}"
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

    def _closes_stale_message(self, silent_seconds: float) -> str:
        return (f"[WATCHDOG] K线闭合停滞告警: kline_closes 已 {silent_seconds / 60:.0f} 分钟无增长"
                f"（阈值 {self.closes_stall_minutes:.0f} 分钟），行情 feed 可能中断，请检查！")

    def _closes_recovery_message(self) -> str:
        return "[WATCHDOG] K线闭合恢复: kline_closes 已恢复增长。"

    def _fail_rate_message(self, rate: float, failed, placed) -> str:
        return (f"[WATCHDOG] 订单失败率告警: 失败率 {rate * 100:.1f}%"
                f" ({failed:.0f}/{placed + failed:.0f} 单) 超过阈值"
                f" {self.fail_rate_threshold * 100:.0f}%，请检查 API/幂等/余额！")

    def _fail_rate_recovery_message(self, rate: float) -> str:
        return f"[WATCHDOG] 订单失败率恢复: 当前失败率 {rate * 100:.1f}%。"


def build_notifier() -> Optional[Any]:
    """根据环境变量构造钉钉通知器；未配置时返回 None（降级日志）。

    webhook 兼容两个环境变量名:
      - DINGTALK_WEBHOOK_URL  (watchdog 现行命名, 优先)
      - DINGTALK_WEBHOOK      (tools/network_monitor/notifier.py 沿用旧名, 兜底)
    secret 统一用 DINGTALK_SECRET。
    """
    webhook = (os.environ.get("DINGTALK_WEBHOOK_URL") or os.environ.get("DINGTALK_WEBHOOK") or "").strip()
    if not webhook:
        logger.warning("DINGTALK_WEBHOOK_URL / DINGTALK_WEBHOOK 均未配置 — 告警降级为本地日志")
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
    parser.add_argument("--closes-stall-minutes", type=float, default=15.0,
                        help="kline_closes 无增长超过该分钟数判定为 K线闭合停滞 (默认 15)")
    parser.add_argument("--fail-rate-threshold", type=float, default=0.10,
                        help="订单失败率超过该比例触发告警 (默认 0.10)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    notifier = build_notifier()
    logger.info("钉钉告警已启用" if notifier is not None else "钉钉未配置，告警降级为日志")

    redis_url = args.redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379")
    watchdog = HeartbeatWatchdog(redis_url, stale_after=args.stale_after,
                                 interval=args.interval, notifier=notifier,
                                 closes_stall_minutes=args.closes_stall_minutes,
                                 fail_rate_threshold=args.fail_rate_threshold)
    logger.info("heartbeat_watchdog 启动: redis=%s stale_after=%.0fs interval=%.0fs "
                "closes_stall=%.0fm fail_rate>%.0f%%",
                redis_url, args.stale_after, args.interval,
                args.closes_stall_minutes, args.fail_rate_threshold * 100)
    watchdog.run_forever()


if __name__ == "__main__":
    main()

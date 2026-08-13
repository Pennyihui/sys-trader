"""proxy_watchdog — Clash 代理健康监控 + 故障时通过 proxy_pool 切换节点。

背景: 主系统依赖 Clash 代理(127.0.0.1:7897)访问 Binance，延迟波动 6-10s
会拖垮签名窗口。本工具周期探测代理延迟，持续超标时调用 proxy_pool
切换 Clash 节点 + 钉钉告警。

切换机制（与 proxy_pool 服务同路径，不产生配置分叉）:
  读取 tools/proxy_pool/proxy_pool.json → clash_updater.apply_config(pool, force_reload=True)
  → 全量重写 mihomo.yaml + 热重载核心。等价于 `proxy_pool.py --generate`；
  proxy_pool 服务自身的健康检查循环（60s）也走同一函数，双方同一数据源、
  同一写入文件，只会互相覆盖为相同内容。
  注意: apply_config 内部是顶层 import（from config_generator import ...），
  因此本工具运行时会把 tools/proxy_pool 加入 sys.path。

告警: DINGTALK_WEBHOOK_URL 环境变量存在时用 monitor.dingtalk.DingTalkNotifier；
缺失时降级为 logger.warning（不中断运行）。

用法: python tools/proxy_watchdog.py [--threshold-ms 5000] [--consecutive 3] [--interval 30]
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

PROBE_URL = "https://testnet.binancefuture.com/fapi/v1/time"
PROXY_URL = "http://127.0.0.1:7897"
POOL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy_pool")
POOL_JSON = os.path.join(POOL_DIR, "proxy_pool.json")
DEFAULT_COOLDOWN = 300  # 切换/告警后冷却（秒）


def _probe_latency_ms(timeout: float = 12.0) -> Optional[float]:
    """经 7897 探测 testnet 时间接口，返回延迟毫秒。

    超时/连接错误/非 200 都视为探测失败，返回 None（计入连续超标）。
    """
    t0 = time.monotonic()
    try:
        resp = requests.get(
            PROBE_URL,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return (time.monotonic() - t0) * 1000.0
        logger.warning("探测返回非 200: %s", resp.status_code)
        return None
    except requests.RequestException as e:
        logger.warning("探测失败: %s", e)
        return None


class ProxyWatchdog:
    """Clash 代理延迟监控 + 故障切换状态机。

    状态机: 一次健康探测清零 bad_streak；探测失败或延迟超标累加 bad_streak；
    当 bad_streak >= consecutive 时触发切换（受 cooldown 去抖，避免反复动作）。
    """

    def __init__(self, threshold_ms: int = 5000, consecutive: int = 3,
                 cooldown: int = DEFAULT_COOLDOWN, probe_timeout: float = 12.0,
                 pool_dir: str = POOL_DIR, notifier=None):
        self.threshold_ms = threshold_ms
        self.consecutive = consecutive
        self.cooldown = cooldown
        self.probe_timeout = probe_timeout
        self.pool_dir = pool_dir
        self.notifier = notifier
        self.bad_streak = 0
        self._last_action_ts = 0.0

    def probe(self) -> Optional[float]:
        """探测一次代理延迟（ms），失败返回 None。"""
        return _probe_latency_ms(timeout=self.probe_timeout)

    def check_once(self) -> dict:
        """执行一轮探测并更新状态机，返回本轮结果（供测试/日志）。

        Returns:
            {"latency_ms", "slow", "bad_streak", "triggered", "switched",
             "notified", "cooldown"}
        """
        latency = self.probe()
        slow = latency is None or latency > self.threshold_ms
        if slow:
            self.bad_streak += 1
        else:
            self.bad_streak = 0
        result = {
            "latency_ms": latency,
            "slow": slow,
            "bad_streak": self.bad_streak,
            "triggered": False,
            "switched": False,
            "notified": False,
            "cooldown": False,
        }
        if self.bad_streak >= self.consecutive:
            self._do_failover(result)
        return result

    def _do_failover(self, result: dict) -> None:
        """达到连续超标阈值后的切换 + 告警（受 cooldown 去抖）。"""
        now = time.time()
        remaining = self.cooldown - (now - self._last_action_ts)
        if remaining > 0:
            result["cooldown"] = True
            logger.info("冷却期内（剩余 %.0fs），跳过重复切换/告警", remaining)
            return
        switched = self.switch_node()
        # 仅切换成功才更新冷却时间戳——失败时要能尽快重试（300s 冷却会拖慢恢复）
        if switched:
            self._last_action_ts = time.time()
        notified = self.notify(result, switched)
        result.update(triggered=True, switched=switched, notified=notified)

    def switch_node(self) -> bool:
        """读 proxy_pool.json → apply_config(force_reload=True) 切换节点。

        与 proxy_pool 服务 --generate / 健康检查循环使用同一入口，
        池子数据取服务自有的 proxy_pool.json（API 服务也是读它）。
        """
        pool = self._load_pool()
        if pool is None:
            logger.error("proxy_pool.json 不可读，无法切换节点")
            return False
        try:
            if self.pool_dir not in sys.path:
                sys.path.insert(0, self.pool_dir)
            from clash_updater import apply_config  # 延迟导入，避免启动依赖
            ok = bool(apply_config(pool, force_reload=True))
            logger.info("Clash 配置切换%s（%d 个节点写入 mihomo.yaml + 热重载）",
                        "成功" if ok else "失败", len(pool.get("proxies", [])))
            return ok
        except Exception as e:
            logger.error("切换节点异常: %s", e)
            return False

    def _load_pool(self) -> Optional[dict]:
        path = os.path.join(self.pool_dir, "proxy_pool.json")
        if not os.path.exists(path):
            logger.error("池子文件不存在: %s", path)
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error("读取池子文件失败: %s", e)
            return None

    def notify(self, result: dict, switched: bool) -> bool:
        """钉钉告警；notifier 未配置时降级为日志（不抛异常）。"""
        if self.notifier is None:
            logger.warning("未配置 DINGTALK_WEBHOOK_URL，跳过钉钉告警"
                           "（切换仍已执行）")
            return False
        latency = result["latency_ms"]
        lat_txt = f"{latency:.0f} ms" if latency is not None else "探测失败（超时/连接错误）"
        # [SysTrader] 前缀：钉钉机器人自定义关键词（大小写敏感），缺失会被 310000 拒绝
        msg = (
            "[SysTrader][proxy-watchdog] Clash 代理持续超标，已触发节点切换\n"
            f"连续 {self.bad_streak} 次探测异常，最近延迟: {lat_txt}\n"
            f"阈值: {self.threshold_ms} ms，切换: "
            f"{'成功' if switched else '失败（请手动处理）'}"
        )
        try:
            return bool(self.notifier.send(msg))
        except Exception as e:
            logger.error("钉钉告警发送失败: %s", e)
            return False


def make_notifier():
    """按环境变量构建钉钉通知器；两个 webhook 变量名均未配置时返回 None。

    webhook 兼容两个环境变量名:
      - DINGTALK_WEBHOOK_URL  (watchdog 现行命名, 优先)
      - DINGTALK_WEBHOOK      (tools/network_monitor/notifier.py 沿用旧名, 兜底)
    secret 统一用 DINGTALK_SECRET。
    """
    url = (os.environ.get("DINGTALK_WEBHOOK_URL") or os.environ.get("DINGTALK_WEBHOOK") or "").strip()
    if not url:
        return None
    try:
        from monitor.dingtalk import DingTalkNotifier
        return DingTalkNotifier(url, secret=os.environ.get("DINGTALK_SECRET", ""))
    except Exception as e:
        logger.warning("DingTalkNotifier 初始化失败，降级为日志: %s", e)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Clash 代理健康监控 + 故障时通过 proxy_pool 切换节点"
    )
    parser.add_argument("--threshold-ms", type=int, default=5000,
                        help="延迟超标阈值（毫秒），超过即计一次异常")
    parser.add_argument("--consecutive", type=int, default=3,
                        help="连续异常次数达到该值才触发切换")
    parser.add_argument("--interval", type=int, default=30,
                        help="探测周期（秒）")
    parser.add_argument("--cooldown", type=int, default=DEFAULT_COOLDOWN,
                        help="切换/告警后的冷却时间（秒）")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    wd = ProxyWatchdog(
        threshold_ms=args.threshold_ms,
        consecutive=args.consecutive,
        cooldown=args.cooldown,
        notifier=make_notifier(),
    )
    logger.info(
        "proxy_watchdog 启动: 阈值=%dms 连续=%d次 周期=%ds 冷却=%ds 探测=%s",
        args.threshold_ms, args.consecutive, args.interval, args.cooldown, PROBE_URL,
    )
    while True:
        result = wd.check_once()
        latency = result["latency_ms"]
        if result["slow"]:
            lat_txt = f"{latency:.0f} ms" if latency is not None else "探测失败"
            logger.warning("延迟异常: %s（阈值 %d ms），连续异常 %d/%d 次",
                           lat_txt, args.threshold_ms,
                           result["bad_streak"], args.consecutive)
        else:
            logger.info("探测正常: %.0f ms，bad_streak 清零", latency)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()

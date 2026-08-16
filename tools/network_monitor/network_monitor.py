#!/usr/bin/env python
"""Network Monitor — 持续网络监控服务。

功能:
  ① 每 60 秒探针: ping 网关 / 223.5.5.5 / Clash 7897 / 代理池 8765
  ② 历史存储: network_history.jsonl (保留 30 天)
  ③ 告警: 状态变化推送到钉钉
  ④ 统计: uptime% / 断连次数 / 延迟均值
  ⑤ HTTP API: 端口 8766

用法:
  python network_monitor.py            # 前台运行
  python network_monitor.py --once     # 只跑一次探针并退出
  python network_monitor.py --stats    # 查看统计
"""

import argparse
import logging
import os
import sys
import threading
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("network_monitor")

PROBE_INTERVAL = 60      # 探针间隔（秒）
API_HOST = "127.0.0.1"
API_PORT = 8766


def run_probe_cycle(notifier) -> dict:
    """执行一轮探针，存储并推送告警。"""
    import probe
    import storage

    result = probe.run_all_probes()
    storage.append_record(result)

    # 告警推送
    if notifier is not None:
        notifier.update(result.get("network_ok", False), result)

    status = "OK" if result.get("network_ok") else "DOWN"
    logger.info(
        "[%s] gw=%.0fms dns=%.0fms clash=%s pool=%s offset=%sms",
        status,
        result.get("gateway_ms") or 0,
        result.get("dns_ms") or 0,
        "Y" if result.get("clash_ok") else "N",
        "Y" if result.get("pool_ok") else "N",
        result.get("binance_offset_ms") if result.get("binance_offset_ms") is not None else "?",
    )
    return result


def cmd_service(no_notify: bool = False):
    """服务模式：探针循环 + HTTP API。"""
    import api_server
    from notifier import create_notifier

    notifier = None if no_notify else create_notifier()

    # 启动 HTTP API
    api_thread = threading.Thread(
        target=api_server.run_api_server,
        args=(API_HOST, API_PORT),
        daemon=True,
        name="api-server",
    )
    api_thread.start()

    logger.info("网络监控服务启动 (探针间隔 %ds)", PROBE_INTERVAL)

    # 首轮立即执行
    latest = run_probe_cycle(notifier)
    api_server.LATEST_PROBE.update(latest)

    # 循环
    while True:
        try:
            time.sleep(PROBE_INTERVAL)
            latest = run_probe_cycle(notifier)
            api_server.LATEST_PROBE.clear()
            api_server.LATEST_PROBE.update(latest)
        except KeyboardInterrupt:
            logger.info("服务已停止")
            break
        except Exception as e:
            logger.error("探针循环异常: %s，60秒后重试", e)
            time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="Network Monitor")
    parser.add_argument("--once", action="store_true", help="只跑一次探针")
    parser.add_argument("--stats", type=int, nargs="?", const=24,
                        metavar="HOURS", help="查看统计")
    parser.add_argument("--timeline", type=int, nargs="?", const=24,
                        metavar="HOURS", help="查看时间线")
    parser.add_argument("--no-notify", action="store_true",
                        help="不推送告警")

    args = parser.parse_args()

    if args.once:
        from notifier import create_notifier
        notifier = None if args.no_notify else create_notifier()
        result = run_probe_cycle(notifier)
        print(json_dumps(result))
        return

    if args.stats is not None:
        import storage
        stats = storage.compute_stats(args.stats)
        print(f"=== 最近 {stats['hours']} 小时网络统计 ===")
        print(f"探针次数: {stats['total_probes']}")
        print(f"可用性:   {stats['uptime_pct']}%")
        print(f"断连事件: {stats['down_events']} 次")
        print(f"网关延迟: {stats['avg_gateway_ms']}ms (均值)")
        print(f"DNS延迟:  {stats['avg_dns_ms']}ms (均值)")
        print(f"当前状态: {stats['last_status']}")
        return

    if args.timeline is not None:
        import storage
        timeline = storage.get_timeline(args.timeline)
        print(f"=== 最近 {args.timeline} 小时时间线 (每分钟) ===")
        for item in timeline:
            mark = "✅" if item["ok"] else "❌"
            print(
                f"{time.strftime('%m-%d %H:%M', time.localtime(item['ts']))} "
                f"{mark} gw={item['gateway_ms']}ms dns={item['dns_ms']}ms"
            )
        return

    cmd_service(no_notify=args.no_notify)


def json_dumps(data: dict) -> str:
    import json
    return json.dumps(data, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
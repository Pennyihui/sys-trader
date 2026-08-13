#!/usr/bin/env python
"""Proxy Pool Manager - 代理池管理主入口。

用法:
  python proxy_pool.py --update      # 更新订阅 + 合并新节点
  python proxy_pool.py --health      # 测速所有节点
  python proxy_pool.py --generate    # 生成配置并应用
  python proxy_pool.py --all         # 完整流程：更新 + 测速 + 生成
  python proxy_pool.py --status      # 查看池子状态
  python proxy_pool.py --service     # Windows 服务模式（HTTP API + 守护循环）
  python proxy_pool.py --install     # 安装到 Windows 任务计划程序
"""

import argparse
import json
import logging
import sys
import os
import time
import threading
import subprocess
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("proxy_pool")

POOL_PATH = os.path.join(os.path.dirname(__file__), "proxy_pool.json")

HEALTH_INTERVAL = 60
UPDATE_INTERVAL = 21600
API_HOST = "127.0.0.1"
API_PORT = 8765


def load_pool() -> dict:
    if not os.path.exists(POOL_PATH):
        logger.warning("池子文件不存在，创建新池子")
        return {
            "version": 1, "last_updated": "",
            "subscription_urls": [
                "https://github.com/Au1rxx/free-vpn-subscriptions/raw/main/output/clash.yaml",
                "https://raw.githubusercontent.com/diplole/proxy-pool/main/clash.yaml",
                "https://raw.githubusercontent.com/diplole/proxy-pool/main/ikuku-free.yaml",
                "https://cdn.jsdelivr.net/gh/free18/v2ray@main/c.yaml",
                "https://v2rayfreeclash.github.io/uploads/2026/08/0-20260802.yaml",
                "https://topfreeclash.github.io/uploads/2026/07/0-20260730.yaml",
                "https://raw.githubusercontent.com/sunmiao4458/free-proxy-airport/main/output/clash.yaml",
                "https://raw.githubusercontent.com/zhuhaiuk/free-nodes/main/clash_config.yaml",
            ],
            "proxies": [],
            "proxy_groups": {
                "auto-failover": {
                    "type": "url-test",
                    "url": "https://fapi.binance.com/fapi/v1/time",
                    "interval": 60, "tolerance": 100,
                }
            },
            "cleanup": {"max_fail_days": 7, "max_fail_count": 10080},
        }
    with open(POOL_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_pool(pool: dict):
    # 原子写：先写临时文件再 os.replace，避免与 watchdog 双进程并发写互相读到半截文件
    tmp_path = POOL_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(pool, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, POOL_PATH)
    logger.info("池子已保存 (%d 个节点)", len(pool.get("proxies", [])))


def cmd_update(pool: dict):
    urls = pool.get("subscription_urls", [])
    if not urls:
        logger.error("未配置订阅地址")
        return
    from subscription import download_all_subscriptions
    from merge import merge_proxies
    new_proxies = download_all_subscriptions(urls)
    cleanup = pool.get("cleanup", {})
    pool = merge_proxies(
        pool, new_proxies,
        max_fail_days=cleanup.get("max_fail_days", 7),
        max_fail_count=cleanup.get("max_fail_count", 10080),
    )
    save_pool(pool)


def cmd_health(pool: dict):
    from health_checker import health_check
    pool = health_check(pool)
    save_pool(pool)


def cmd_generate(pool: dict, skip_restart: bool = False):
    """生成完整 mihomo 配置并应用。

    Args:
        skip_restart: True = 仅写入，配置变化时才热重载（健康检查循环）；
                      False = 强制热重载（订阅更新 / 手动 --generate）
    """
    from clash_updater import apply_config
    if not apply_config(pool, force_reload=not skip_restart):
        logger.error("配置应用失败")


def cmd_status(pool: dict):
    proxies = pool.get("proxies", [])
    healthy = [p for p in proxies if p.get("healthy")]
    unhealthy = [p for p in proxies if not p.get("healthy")]
    urls = pool.get("subscription_urls", [])
    print(f"\n===== Proxy Pool Status =====")
    print(f"最后更新: {pool.get('last_updated', '从未')}")
    print(f"订阅地址: {len(urls)} 个")
    for u in urls:
        print(f"  - {u}")
    print(f"总计节点: {len(proxies)}")
    print(f"可用节点: {len(healthy)}")
    print(f"不可用节点: {len(unhealthy)}")
    print(f"清理阈值: {pool.get('cleanup', {}).get('max_fail_days', 7)} 天")
    print()
    if healthy:
        print("--- 可用节点 ---")
        for p in healthy[:10]:
            print(f"  [+] {p['name']} ({p['type']})")
        if len(healthy) > 10:
            print(f"  ... 还有 {len(healthy) - 10} 个")
    if unhealthy:
        print("--- 不可用节点 ---")
        for p in unhealthy[:5]:
            print(f"  [-] {p['name']} ({p.get('fail_count', 0)}次失败)")
        if len(unhealthy) > 5:
            print(f"  ... 还有 {len(unhealthy) - 5} 个")
    print("==============================\n")


def cmd_service():
    from api_server import run_api_server
    api_thread = threading.Thread(
        target=run_api_server, args=(API_HOST, API_PORT),
        daemon=True, name="api-server",
    )
    api_thread.start()
    logger.info("HTTP API 已启动: http://%s:%s", API_HOST, API_PORT)

    logger.info("首次执行: 更新 + 测速 + 生成")
    try:
        # 先保证核心在线（订阅下载要走代理），再跑首次流程
        from core_manager import watchdog
        watchdog()
        pool = load_pool()
        cmd_update(pool); pool = load_pool()
        cmd_health(pool); pool = load_pool()
        cmd_generate(pool, skip_restart=True)
    except Exception as e:
        logger.error("首次执行失败: %s", e)

    last_update = 0
    last_health = 0
    logger.info("守护循环启动，健康检查间隔=%ds，订阅更新间隔=%ds", HEALTH_INTERVAL, UPDATE_INTERVAL)

    # 模块在循环外导入一次——不要在循环内 importlib.reload：
    # reload 会重置 health_checker._probe_round（环形探测窗口固定为前 800 个节点，
    # ~1400 个健康节点永不探测）和 clash_updater._last_written_hash（热重载去重失效，
    # 每 60s 强制重载抖动）。改动代码后重启服务生效即可。
    from health_checker import health_check  # noqa: F401
    from clash_updater import apply_config  # noqa: F401
    from core_manager import watchdog  # noqa: F401

    while True:
        now = time.time()
        try:
            if now - last_update >= UPDATE_INTERVAL:
                logger.info("定时: 更新订阅")
                pool = load_pool()
                cmd_update(pool)
                last_update = now
                # 订阅更新后强制重载（节点集合变了）
                cmd_generate(pool, skip_restart=False)
            if now - last_health >= HEALTH_INTERVAL:
                logger.info("定时: 健康检查")
                pool = load_pool()
                cmd_health(pool)
                last_health = now
                cmd_generate(pool, skip_restart=True)
            # 看门狗：核心不在线（7897 不通）就拉起
            watchdog()
            time.sleep(10)
        except KeyboardInterrupt:
            logger.info("服务已停止")
            break
        except Exception as e:
            logger.error("服务异常: %s，60秒后重试", e)
            time.sleep(60)


def cmd_install():
    script_path = os.path.abspath(__file__)
    python_path = sys.executable
    task_name = "ProxyPoolManager"
    schedule_cmd = (
        f'schtasks /create /tn "{task_name}-schedule" /sc HOURLY '
        f'/mo 6 /tr "{python_path} {script_path} --all" '
        f'/ru "%USERNAME%" /f /it'
    )
    logger.info("执行: %s", schedule_cmd)
    result = subprocess.run(schedule_cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        logger.info("计划任务创建成功")
    else:
        logger.warning("计划任务创建失败: %s", result.stderr.strip())


def main():
    parser = argparse.ArgumentParser(description="Proxy Pool Manager")
    parser.add_argument("--update", action="store_true", help="更新订阅")
    parser.add_argument("--health", action="store_true", help="测速")
    parser.add_argument("--generate", action="store_true", help="生成配置")
    parser.add_argument("--all", action="store_true", help="完整流程")
    parser.add_argument("--status", action="store_true", help="状态")
    parser.add_argument("--no-restart", action="store_true", help="不重启核心")
    parser.add_argument("--service", action="store_true", help="服务模式")
    parser.add_argument("--install", action="store_true", help="安装到任务计划程序")

    args = parser.parse_args()

    if args.service:
        cmd_service()
        return
    if args.install:
        cmd_install()
        return
    if not any([args.update, args.health, args.generate, args.all, args.status]):
        parser.print_help()
        return

    pool = load_pool()
    if args.all:
        logger.info("===== 完整流程开始 =====")
        cmd_update(pool); pool = load_pool()
        cmd_health(pool); pool = load_pool()
        cmd_generate(pool, skip_restart=args.no_restart)
        logger.info("===== 完整流程完成 =====")
    else:
        if args.update: cmd_update(pool); pool = load_pool()
        if args.health: cmd_health(pool); pool = load_pool()
        if args.generate: cmd_generate(pool, skip_restart=args.no_restart)
        if args.status: cmd_status(pool)


if __name__ == "__main__":
    main()
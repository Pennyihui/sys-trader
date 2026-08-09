"""离线模拟运行器 — ReplayFeed 驱动 SystemRunner（DRY_RUN）跑历史数据。

用法: python tools/replay_runner.py --data data/replay --symbols BTCUSDT --hours 168
验收: 全量重放无异常 + 起止 RSS 平稳（内存泄漏判定）。

数据准备: data/replay/BTCUSDT_15m.json (JSON 数组, 每元素含
open/high/low/close/volume/open_time), 每个 symbol 一个文件。

说明:
  - initialize() 仍走启动前校验 (preflight) 与 stepSize 拉取 —— 需要 testnet
    可达 (经 127.0.0.1:7897 代理) 与 config/.env 中的 API Key。
  - 行情侧完全离线: 无 WebSocket, 不连市场数据。
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.config_loader import load_env
from shared.logging import setup_logging
from shared.runner import SystemRunner
from tools.replay_feed import ReplayFeed

import signal_engine.scalping_strategy  # noqa: F401 注册15m剥头皮策略 (StrategyRegistry)


def rss_mb() -> float:
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except ImportError:
        return 0.0


def main():
    parser = argparse.ArgumentParser(description="离线模拟（历史K线重放）")
    parser.add_argument("--data", default="data/replay")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--strategy", default="scalping_15m")
    parser.add_argument("--hours", type=int, default=168)
    args = parser.parse_args()
    load_env()
    setup_logging(log_dir="logs", json_console=False)

    runner = SystemRunner(
        testnet=True, symbols=args.symbols.split(","),
        strategy_name=args.strategy, execution_mode_name="dry_run", hours=args.hours,
    )
    # 用 ReplayFeed 替换真实 feed（不下单、不连 WS）
    runner.feed = ReplayFeed(
        data_dir=args.data, symbols=runner.symbols,
        on_kline_closed=runner._on_kline_closed,
    )
    runner.initialize()
    rss_before = rss_mb()
    runner.feed.run_once()
    rss_after = rss_mb()
    runner.report()
    growth = rss_after - rss_before
    logger = logging.getLogger("replay")
    logger.info("RSS before=%.1fMB after=%.1fMB growth=%.1fMB", rss_before, rss_after, growth)
    if growth > 50:
        logger.warning("疑似内存泄漏（RSS 增长 %.1fMB）", growth)
        sys.exit(2)
    logger.info("重放完成，无异常")


if __name__ == "__main__":
    main()

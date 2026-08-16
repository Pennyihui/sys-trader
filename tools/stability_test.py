"""稳定性测试入口 — SystemRunner 的 thin wrapper（testnet 真实下单）。

用法不变: python tools/stability_test.py --hours 24
系统装配见 shared/runner.py SystemRunner（唯一完整装配）。
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.config_loader import load_env
from shared.logging import setup_logging
from shared.runner import SystemRunner


def main():
    load_env()
    parser = argparse.ArgumentParser(description="稳定性测试 (testnet下单)")
    parser.add_argument("--hours", type=float, default=24,
                        help="运行时长(小时, 支持小数如 0.17=10分钟, OPS-002)")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--strategy", default="scalping_15m")
    args = parser.parse_args()
    setup_logging(log_dir="logs", json_console=False)
    from shared.event_bus import EventBus
    event_bus = EventBus(redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"))
    runner = SystemRunner(
        testnet=True, symbols=args.symbols.split(","),
        strategy_name=args.strategy, hours=args.hours,
        event_bus=event_bus,
    )
    try:
        runner.initialize()
        runner.run_forever()
    except Exception:
        logging.getLogger("stability").exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()

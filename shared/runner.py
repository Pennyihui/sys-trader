"""系统主入口 — 启动所有模块、优雅关闭、状态同步。"""

import logging
import signal
import sys
import time
from typing import Optional

from execution.order_gateway import OrderGateway
from portfolio.tracker import PortfolioTracker
from market_data.feed import MarketDataFeed
from shared.startup_reconciler import StartupReconciler
from shared.logging import setup_logging

logger = logging.getLogger(__name__)


class SystemRunner:
    """交易系统主控 — 管理模块生命周期。"""

    def __init__(self, testnet: bool = True):
        self.testnet = testnet
        self._running = False
        self.feed: Optional[MarketDataFeed] = None
        self.portfolio: Optional[PortfolioTracker] = None
        self.gateway: Optional[OrderGateway] = None

        # 注册信号处理
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, shutting down...", sig_name)
        self.stop()

    def initialize(self):
        """初始化所有模块。"""
        logger.info("Initializing system (testnet=%s)...", self.testnet)
        self.gateway = OrderGateway(testnet=self.testnet)
        self.portfolio = PortfolioTracker()
        self.feed = MarketDataFeed(
            symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            proxy_host="127.0.0.1",
            proxy_port=7897,
        )

        # 启动行情
        self.feed.start()
        time.sleep(2)

        # 查询账户
        try:
            acc = self.gateway.get_account()
            if "canTrade" in acc:
                total = sum(
                    float(a.get("walletBalance", 0))
                    for a in acc.get("assets", [])
                )
                self.portfolio.update_equity(total)
                logger.info("Account equity: %.2f USDT", total)
        except Exception as e:
            logger.warning("Account fetch failed: %s", e)

        # 对账
        reconciler = StartupReconciler(self.gateway, self.portfolio)
        reconciler.reconcile()

        self._running = True
        logger.info("System initialized")

    def run_forever(self):
        """主循环 — 持续运行直到收到信号。"""
        logger.info("System running (PID=%d)", os.getpid())
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """优雅关闭所有模块。"""
        logger.info("Shutting down...")
        self._running = False
        if self.feed:
            self.feed.stop()
        logger.info("Shutdown complete")

    @property
    def healthy(self) -> bool:
        return (
            self._running
            and self.feed is not None
            and self.feed.get_last_price("BTCUSDT") is not None
        )


import os


def main():
    setup_logging()
    runner = SystemRunner(testnet=True)
    try:
        runner.initialize()
        runner.run_forever()
    except Exception as e:
        logger.exception("Fatal error")
        sys.exit(1)
    finally:
        runner.stop()


if __name__ == "__main__":
    main()

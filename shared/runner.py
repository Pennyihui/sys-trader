"""系统主入口 — 启动前校验、订单幂等、持续对账、优雅关闭。"""

import logging
import os
import signal
import sys
import time
from typing import Optional

from execution.order_gateway import OrderGateway
from portfolio.tracker import PortfolioTracker
from market_data.feed import MarketDataFeed
from shared.idempotency import IdempotencyTracker
from shared.preflight import PreflightChecker
from shared.reconciler import PositionReconciler
from shared.startup_reconciler import StartupReconciler
from shared.logging import setup_logging

logger = logging.getLogger(__name__)


class SystemRunner:
    """交易系统主控 — 管理模块生命周期。"""

    def __init__(self, testnet: bool = True, db_path: str = "data/intents.db"):
        self.testnet = testnet
        self._running = False
        self.feed: Optional[MarketDataFeed] = None
        self.portfolio: Optional[PortfolioTracker] = None
        self.gateway: Optional[OrderGateway] = None
        self.idempotency: Optional[IdempotencyTracker] = None
        self.reconciler: Optional[PositionReconciler] = None

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
        self.idempotency = IdempotencyTracker(db_path=os.environ.get(
            "INTENTS_DB_PATH", "data/intents.db"
        ))

        # 启动前校验
        preflight = PreflightChecker(self.gateway)
        if not preflight.run_all():
            logger.error("Preflight checks failed — aborting")
            sys.exit(1)
        logger.info("All preflight checks passed")

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

        # 启动时对账 — 检查本地持仓 vs 交易所
        startup_rec = StartupReconciler(self.gateway, self.portfolio)
        startup_rec.reconcile()

        # 持续对账 — 后台定期检查
        self.reconciler = PositionReconciler(self.gateway, self.portfolio)
        self.reconciler.start()

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

        if self.reconciler:
            self.reconciler.stop()
        if self.feed:
            self.feed.stop()
        if self.idempotency:
            self.idempotency.close()
        logger.info("Shutdown complete")

    @property
    def healthy(self) -> bool:
        return (
            self._running
            and self.feed is not None
            and self.feed.get_last_price("BTCUSDT") is not None
        )


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

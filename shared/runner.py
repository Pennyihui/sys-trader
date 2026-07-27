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
from shared.logging import setup_logging

logger = logging.getLogger(__name__)


class SystemRunner:
    """交易系统主控 — 管理模块生命周期。"""

    def __init__(self, testnet: bool = True):
        self.testnet = testnet
        self.feed: Optional[MarketDataFeed] = None
        self.portfolio: Optional[PortfolioTracker] = None
        self.gateway: Optional[OrderGateway] = None
        self.idempotency: Optional[IdempotencyTracker] = None
        self.reconciler: Optional[PositionReconciler] = None

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info("Received %s, shutting down...", signal.Signals(signum).name)
        self.stop()

    def initialize(self):
        self.gateway = OrderGateway(testnet=self.testnet)
        self.portfolio = PortfolioTracker()
        self.feed = MarketDataFeed(
            symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            proxy_host="127.0.0.1", proxy_port=7897,
        )
        self.idempotency = IdempotencyTracker(
            db_path=os.environ.get("INTENTS_DB_PATH", "data/intents.db")
        )

        # 启动前校验 (单次 get_account, 缓存结果)
        preflight = PreflightChecker(self.gateway)
        acc = preflight.run_all()
        if acc is None:
            raise RuntimeError("Preflight checks failed")

        # 用校验时的账户数据初始化权益, 无需再调 API
        total = sum(float(a.get("walletBalance", 0)) for a in acc.get("assets", []))
        self.portfolio.update_equity(total)
        logger.info("Account equity: %.2f USDT", total)

        # 启动行情
        self.feed.start()
        time.sleep(2)

        # 启动时对账 (使用缓存的账户数据)
        reconciler = PositionReconciler(self.gateway, self.portfolio)
        reconciler.reconcile(cached_account=acc)

        # 持续对账
        self.reconciler = reconciler
        self.reconciler.start()
        logger.info("System initialized")

    def run_forever(self):
        logger.info("System running (PID=%d)", os.getpid())
        while True:
            time.sleep(1)

    def stop(self):
        logger.info("Shutting down...")
        if self.reconciler:
            self.reconciler.stop()
        if self.feed:
            self.feed.stop()
        if self.idempotency:
            self.idempotency.close()
        logger.info("Shutdown complete")
        sys.exit(0)

    @property
    def healthy(self) -> bool:
        return (self.feed is not None
                and self.feed.get_last_price("BTCUSDT") is not None)


def main():
    setup_logging()
    runner = SystemRunner(testnet=True)
    try:
        runner.initialize()
        runner.run_forever()
    except Exception as e:
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()

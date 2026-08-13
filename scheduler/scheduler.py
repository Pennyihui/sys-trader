"""Scheduler — dispatches kline.closed events to SignalEngine via thread pool."""

import logging
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, engine_run: Callable, max_workers: int = 8):
        self.engine_run = engine_run
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: List[Future] = []

    def _consume_result(self, future: Future) -> None:
        """取回任务结果, 触发异常时记日志 (避免线程池静默吞掉异常)。"""
        try:
            future.result()
        except Exception as e:
            logger.error("Scheduled engine_run failed: %s", e, exc_info=True)

    def dispatch(self, symbol: str, timeframe: str, ohlcv: list) -> None:
        future = self._executor.submit(self.engine_run, symbol, timeframe, ohlcv)
        future.add_done_callback(self._consume_result)
        self._futures = [f for f in self._futures if not f.done()]
        self._futures.append(future)

    def on_kline_closed(self, symbol: str, timeframe: str, ohlcv: list):
        self.dispatch(symbol, timeframe, ohlcv)

    def shutdown(self, wait: bool = True):
        self._executor.shutdown(wait=wait)

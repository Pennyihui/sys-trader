"""Scheduler — dispatches kline.closed events to SignalEngine via thread pool."""

from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, List, Optional


class Scheduler:
    def __init__(self, engine_run: Callable, max_workers: int = 8):
        self.engine_run = engine_run
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: List[Future] = []

    def dispatch(self, symbol: str, timeframe: str, ohlcv: list) -> None:
        future = self._executor.submit(self.engine_run, symbol, timeframe, ohlcv)
        self._futures = [f for f in self._futures if not f.done()]
        self._futures.append(future)

    def on_kline_closed(self, symbol: str, timeframe: str, ohlcv: list):
        self.dispatch(symbol, timeframe, ohlcv)

    def shutdown(self, wait: bool = True):
        self._executor.shutdown(wait=wait)

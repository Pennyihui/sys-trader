"""Middleware chain — composable risk checks executed in order."""

from dataclasses import dataclass, field
from typing import Any, Dict, List
from signal_engine.engine import Signal
from portfolio.tracker import PortfolioTracker


@dataclass
class MiddlewareResult:
    rejected: bool
    signal: Signal | None = None
    reason: str = ""
    modifications: Dict[str, Any] = field(default_factory=dict)


class Middleware:
    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        raise NotImplementedError


class MiddlewareChain:
    def __init__(self):
        self._middleware: List[Middleware] = []

    def add(self, mw: Middleware):
        self._middleware.append(mw)
        return self

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        current_signal = signal
        for mw in self._middleware:
            result = mw.process(current_signal, portfolio)
            if result.rejected:
                return result
            if result.signal is not None:
                current_signal = result.signal
        return MiddlewareResult(rejected=False, signal=current_signal)

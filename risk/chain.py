"""Middleware chain — composable risk checks executed in order."""

import inspect
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
    def process(self, signal: Signal, portfolio: PortfolioTracker,
                modifications: Dict[str, Any] = None) -> MiddlewareResult:
        raise NotImplementedError


class MiddlewareChain:
    def __init__(self, event_bus=None, instance="live"):
        self._middleware: List[Middleware] = []
        self._event_bus = event_bus  # 事件总线注入（可选，None 时静默）
        self.instance = instance  # 实例标识: live / paper / dry_run

    def add(self, mw: Middleware):
        self._middleware.append(mw)
        return self

    def process(self, signal: Signal, portfolio: PortfolioTracker) -> MiddlewareResult:
        current_signal = signal
        modifications: Dict[str, Any] = {}
        for mw in self._middleware:
            # 累计 modifications 透传给后续中间件 (如拟开仓 position_size),
            # 使集中度检查能把本次拟开仓保证金计入判断。
            # 兼容旧式两参中间件 (process(signal, portfolio)): 通过签名探测降级。
            sig = inspect.signature(mw.process)
            accepts_mods = (len(sig.parameters) >= 3
                            or any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values()))
            if accepts_mods:
                result = mw.process(current_signal, portfolio, modifications)
            else:
                result = mw.process(current_signal, portfolio)
            if result.rejected:
                # 埋点: 任一中件间拒绝 → signal.rejected（event_bus 为 None 时静默）
                if self._event_bus is not None:
                    self._event_bus.publish("signal.rejected", {
                        "instance": self.instance, "symbol": signal.symbol,
                        "direction": signal.direction, "reason": result.reason,
                        "signal_id": signal.signal_id,
                    })
                return result
            if result.signal is not None:
                current_signal = result.signal
            modifications.update(result.modifications)
        # 埋点: 全部通过 → signal.approved（modifications 为风控修改，如 position_size）
        if self._event_bus is not None:
            self._event_bus.publish("signal.approved", {
                "instance": self.instance, "symbol": signal.symbol,
                "direction": signal.direction, "modifications": modifications,
                "signal_id": signal.signal_id,
            })
        return MiddlewareResult(rejected=False, signal=current_signal, modifications=modifications)

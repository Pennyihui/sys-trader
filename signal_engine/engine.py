"""SignalEngine — unified entry point for 4-layer signal generation."""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

# 仅在类型检查时导入，避免与 interface.py 的顶层 import 形成循环依赖
if TYPE_CHECKING:
    from signal_engine.interface import IStrategy


@dataclass
class Signal:
    symbol: str
    direction: str  # LONG / SHORT
    conviction: float
    entry_price: float
    stop_loss: float
    take_profit: float
    attribution: Dict[str, float] = field(default_factory=dict)
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    leverage: int = 3  # 策略声明的杠杆（风控链 LeverageController 校验用）


class SignalEngine:
    def __init__(self, strategy: Optional["IStrategy"] = None,
                 event_bus=None, instance: str = "live"):
        self._weekly_cache: Dict[str, Any] = {}
        self._daily_cache: Dict[str, Any] = {}
        self.strategy = strategy  # 可插拔策略
        self.event_bus = event_bus  # 事件总线注入（可选，None 时静默）
        self.instance = instance  # 实例标识: live / paper / dry_run

    def run(self, symbol: str, timeframe: str, ohlcv: List[dict]) -> Optional[Signal]:
        if not ohlcv:
            return None
        # 防御: 调用方若透传 is_closed 标志, 只保留已闭合 K 线求值——
        # 未闭合 (forming) 蜡烛的末行数据是部分的, 不能作为信号依据 (2026-08-16 审计修复)。
        if isinstance(ohlcv[0], dict) and "is_closed" in ohlcv[0]:
            ohlcv = [row for row in ohlcv if row.get("is_closed")]
            if not ohlcv:
                return None
        signal = None
        # 有策略且 timeframe 匹配时，走策略分析（支持任意策略时间框架）
        if self.strategy is not None and timeframe == self.strategy.timeframe:
            df = pd.DataFrame(ohlcv)
            signal = self.strategy.analyze(symbol, df)
            # 策略声明的杠杆随信号下发, 供风控链 LeverageController 校验
            if signal is not None and hasattr(self.strategy, "leverage"):
                try:
                    signal.leverage = int(self.strategy.leverage(symbol))
                except Exception:
                    pass
        # 无策略或 timeframe 不匹配时回退到原有逻辑
        elif timeframe == "1w":
            signal = self._run_weekly(symbol, ohlcv)
        elif timeframe == "1d":
            signal = self._run_daily(symbol, ohlcv)
        elif timeframe == "4h":
            signal = self._run_4h(symbol, ohlcv)
        else:
            # timeframe 不匹配且无对应层: 显式告警, 不再静默 (BUG-001 教训)
            logger = logging.getLogger(__name__)
            logger.warning("SignalEngine.run: no handler for timeframe %s", timeframe)
        # 统一出口埋点: 产出 Signal 时发布 signal.generated 事件
        # signal_id 是跨事件流 (signal.generated ↔ order.filled) 的唯一关联键
        if signal is not None and self.event_bus is not None:
            self.event_bus.publish("signal.generated", {
                "instance": self.instance, "symbol": signal.symbol,
                "direction": signal.direction, "conviction": signal.conviction,
                "entry_price": signal.entry_price, "stop_loss": signal.stop_loss,
                "take_profit": signal.take_profit, "signal_id": signal.signal_id,
                "strategy": getattr(self.strategy, "name", ""),
            })
        return signal

    def _run_weekly(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def _run_daily(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def _run_4h(self, symbol: str, ohlcv: List[dict]) -> Optional[Signal]:
        return None

    def get_weekly_context(self, symbol: str) -> Optional[Any]:
        return self._weekly_cache.get(symbol)

    def get_daily_context(self, symbol: str) -> Optional[Any]:
        return self._daily_cache.get(symbol)

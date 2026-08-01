"""运行模式 — DRY_RUN / PAPER / LIVE 三态切换。"""

import logging
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class ExecutionMode(str, Enum):
    DRY_RUN = "dry_run"   # 不产生任何订单
    PAPER = "paper"       # 模拟成交
    LIVE = "live"         # 真实下单


class ExecutionModeManager:
    """运行模式管理 — 启动时固定，运行中可查询。"""

    def __init__(self, mode: ExecutionMode = ExecutionMode.DRY_RUN):
        self.mode = mode
        logger.info("Execution mode: %s", mode.value)

    @classmethod
    def from_env(cls) -> "ExecutionModeManager":
        import os
        mode_str = os.environ.get("EXECUTION_MODE", "dry_run").lower()
        try:
            mode = ExecutionMode(mode_str)
        except ValueError:
            logger.error("Invalid EXECUTION_MODE: %s (use dry_run/paper/live)", mode_str)
            raise
        return cls(mode)

    def is_live(self) -> bool:
        return self.mode == ExecutionMode.LIVE

    def is_paper(self) -> bool:
        return self.mode == ExecutionMode.PAPER

    def can_trade(self) -> bool:
        """是否允许产生订单（paper/live 都可以，dry_run 不行）。"""
        return self.mode in (ExecutionMode.PAPER, ExecutionMode.LIVE)

    def describe(self) -> str:
        return {
            ExecutionMode.DRY_RUN: "只读模式，不产生任何订单",
            ExecutionMode.PAPER: "模拟成交，使用 PaperTrader",
            ExecutionMode.LIVE: "真实下单，连接交易所",
        }[self.mode]

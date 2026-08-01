"""配置校验 — Pydantic schema，启动时 fail-fast。"""

import os
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv

load_dotenv()


class RiskSettings(BaseModel):
    risk_per_trade: float = Field(0.015, gt=0, le=0.1)
    max_leverage: int = Field(5, ge=1, le=20)
    max_position_per_symbol: float = Field(0.30, gt=0, le=1.0)
    max_same_direction: float = Field(0.50, gt=0, le=1.0)
    max_total_margin: float = Field(0.80, gt=0, le=1.0)
    max_drawdown: float = Field(0.15, gt=0, le=1.0)
    daily_loss_limit: float = Field(0.05, gt=0, le=1.0)
    consecutive_loss_breaker: int = Field(3, ge=1)
    cooldown_minutes: int = Field(120, ge=0)


class ExecutionSettings(BaseModel):
    testnet: bool = True
    order_timeout_seconds: int = Field(60, ge=1)
    partial_fill_wait_seconds: int = Field(30, ge=1)
    max_retries: int = Field(3, ge=1, le=10)
    retry_backoff_base: float = Field(1.0, ge=0.1)


class MarketSettings(BaseModel):
    symbols_primary: List[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    proxy_host: str = "127.0.0.1"
    proxy_port: int = Field(7897, ge=1, le=65535)
    redundant_connections: int = Field(4, ge=1, le=8)

    @field_validator("symbols_primary")
    @classmethod
    def validate_symbols(cls, v: List[str]) -> List[str]:
        for s in v:
            if not s.endswith("USDT") or not s.isupper():
                raise ValueError(f"Invalid symbol: {s} (must be uppercase ending in USDT)")
        return v


class AppSettings(BaseModel):
    risk: RiskSettings = Field(default_factory=RiskSettings)
    execution: ExecutionSettings = Field(default_factory=ExecutionSettings)
    market: MarketSettings = Field(default_factory=MarketSettings)


def load_settings() -> AppSettings:
    """加载并校验所有配置，错误立即抛出。"""
    return AppSettings()

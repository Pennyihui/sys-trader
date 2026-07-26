"""Configuration loader -- reads YAML configs into typed objects."""

import os
from dataclasses import dataclass
from typing import List
from pathlib import Path

import yaml
from dotenv import load_dotenv


def load_env(env_path: str = "config/.env") -> None:
    """加载 .env 文件到环境变量。

    .env 文件中存放敏感密钥（API Key、Secret 等），
    不提交到 git，仅供本地使用。

    用法:
        from shared.config_loader import load_env
        load_env()  # 加载 config/.env
    """
    path = Path(env_path)
    if path.exists():
        load_dotenv(path, override=False)
    else:
        import warnings
        warnings.warn(
            f".env 文件不存在: {path.resolve()}"
            f"\n请复制 {path.parent / '.env.example'} 为 {path} 并填入你的密钥"
        )


def load_yaml_config(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_symbols(path: str) -> List[str]:
    config = load_yaml_config(path)
    primary = config.get("symbols", {}).get("primary", [])
    secondary = config.get("symbols", {}).get("secondary", [])
    return primary + secondary


@dataclass
class RiskConfig:
    risk_per_trade: float = 0.015
    max_leverage: int = 5
    max_position_per_symbol: float = 0.30
    max_same_direction: float = 0.50
    max_total_margin: float = 0.80
    max_drawdown: float = 0.15
    daily_loss_limit: float = 0.05
    consecutive_loss_breaker: int = 3
    cooldown_minutes: int = 120


def load_risk_config(path: str) -> RiskConfig:
    config = load_yaml_config(path)
    risk = config.get("risk", {})
    return RiskConfig(
        risk_per_trade=float(risk.get("risk_per_trade", 0.015)),
        max_leverage=int(risk.get("max_leverage", 5)),
        max_position_per_symbol=float(risk.get("max_position_per_symbol", 0.30)),
        max_same_direction=float(risk.get("max_same_direction", 0.50)),
        max_total_margin=float(risk.get("max_total_margin", 0.80)),
        max_drawdown=float(risk.get("max_drawdown", 0.15)),
        daily_loss_limit=float(risk.get("daily_loss_limit", 0.05)),
        consecutive_loss_breaker=int(risk.get("consecutive_loss_breaker", 3)),
        cooldown_minutes=int(risk.get("cooldown_minutes", 120)),
    )

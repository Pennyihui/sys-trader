"""测试配置校验。"""
import pytest
from pydantic import ValidationError
from config.settings import (
    AppSettings, RiskSettings, ExecutionSettings, MarketSettings,
    load_settings,
)


class TestSettings:
    def test_default_settings_valid(self):
        s = load_settings()
        assert s.risk.risk_per_trade == 0.015
        assert s.execution.testnet is True
        assert "BTCUSDT" in s.market.symbols_primary

    def test_risk_per_trade_bounds(self):
        with pytest.raises(ValidationError):
            RiskSettings(risk_per_trade=0.5)  # 超 10%
        with pytest.raises(ValidationError):
            RiskSettings(risk_per_trade=-0.01)

    def test_leverage_bounds(self):
        with pytest.raises(ValidationError):
            RiskSettings(max_leverage=100)
        assert RiskSettings(max_leverage=20).max_leverage == 20

    def test_symbol_validation(self):
        with pytest.raises(ValidationError):
            MarketSettings(symbols_primary=["btcusdt"])  # 小写
        with pytest.raises(ValidationError):
            MarketSettings(symbols_primary=["BTC"])  # 无 USDT 后缀

    def test_proxy_port_bounds(self):
        with pytest.raises(ValidationError):
            MarketSettings(proxy_port=99999)

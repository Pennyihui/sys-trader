"""测试手续费模型。"""
import pytest
from shared.fee_model import FeeModel, FeeConfig


class TestFeeModel:
    def test_market_order_cost(self):
        model = FeeModel()
        cost = model.estimate_cost("MARKET", 0.1, 60000.0)
        assert cost["fee"] == pytest.approx(3.0, rel=0.01)  # 0.1*60000*0.0005
        assert cost["slippage"] == pytest.approx(1.8, rel=0.01)
        assert cost["total_cost"] == pytest.approx(4.8, rel=0.01)

    def test_limit_order_no_slippage(self):
        model = FeeModel()
        cost = model.estimate_cost("LIMIT", 0.1, 60000.0)
        assert cost["slippage"] == 0.0
        assert cost["fee"] == pytest.approx(1.2, rel=0.01)  # maker fee

    def test_custom_config(self):
        config = FeeConfig(taker_fee_pct=0.001, slippage_pct=0.0)
        model = FeeModel(config)
        cost = model.estimate_cost("MARKET", 0.1, 60000.0)
        assert cost["fee"] == pytest.approx(6.0, rel=0.01)

import pytest
from unittest.mock import MagicMock
from shared.preflight import PreflightChecker


class TestPreflight:
    def test_all_checks_pass_with_valid_account(self):
        gw = MagicMock()
        gw.get_account.return_value = {
            "canTrade": True,
            "assets": [{"asset": "USDT", "walletBalance": "10000"}],
        }
        checker = PreflightChecker(gw)
        result = checker.run_all()
        assert result is not None
        assert "canTrade" in result

    def test_fails_when_cant_trade(self):
        gw = MagicMock()
        gw.get_account.return_value = {
            "canTrade": False,
            "assets": [{"asset": "USDT", "walletBalance": "10000"}],
        }
        checker = PreflightChecker(gw)
        assert checker.run_all() is None

    def test_fails_on_network_error(self):
        gw = MagicMock()
        gw.get_account.side_effect = Exception("Connection refused")
        checker = PreflightChecker(gw)
        assert checker.run_all() is None
        results = checker.results
        assert all(r.passed is False for r in results)

    def test_get_account_called_once(self):
        """验证只调用了一次 get_account"""
        gw = MagicMock()
        gw.get_account.return_value = {
            "canTrade": True,
            "assets": [{"asset": "USDT", "walletBalance": "10000"}],
        }
        checker = PreflightChecker(gw)
        checker.run_all()
        assert gw.get_account.call_count == 1

    def test_missing_can_trade_key_marks_reachable_failed(self):
        """账户响应缺 canTrade 键: account_reachable 标记失败, 不静默跳过。"""
        gw = MagicMock()
        gw.get_account.return_value = {"assets": [{"asset": "USDT", "walletBalance": "10000"}]}
        checker = PreflightChecker(gw)
        assert checker.run_all() is None
        passed = {r.name: r.passed for r in checker.results}
        assert passed["account_reachable"] is False
        assert passed["can_trade"] is False

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
        assert checker.run_all() is True

    def test_fails_when_cant_trade(self):
        gw = MagicMock()
        gw.get_account.return_value = {
            "canTrade": False,
            "assets": [{"asset": "USDT", "walletBalance": "10000"}],
        }
        checker = PreflightChecker(gw)
        assert checker.run_all() is False

    def test_fails_on_network_error(self):
        gw = MagicMock()
        gw.get_account.side_effect = Exception("Connection refused")
        checker = PreflightChecker(gw)
        assert checker.run_all() is False
        results = checker.results
        assert all(r.passed is False for r in results)

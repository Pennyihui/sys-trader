"""测试运行模式。"""
import pytest
from shared.execution_mode import ExecutionMode, ExecutionModeManager


class TestExecutionMode:
    def test_default_is_dry_run(self):
        assert ExecutionModeManager().mode == ExecutionMode.DRY_RUN

    def test_can_trade_modes(self):
        assert ExecutionModeManager(ExecutionMode.PAPER).can_trade()
        assert ExecutionModeManager(ExecutionMode.LIVE).can_trade()
        assert not ExecutionModeManager(ExecutionMode.DRY_RUN).can_trade()

    def test_is_live(self):
        assert ExecutionModeManager(ExecutionMode.LIVE).is_live()
        assert not ExecutionModeManager(ExecutionMode.PAPER).is_live()

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("EXECUTION_MODE", "paper")
        mgr = ExecutionModeManager.from_env()
        assert mgr.is_paper()

    def test_from_env_invalid(self, monkeypatch):
        monkeypatch.setenv("EXECUTION_MODE", "bogus")
        with pytest.raises(ValueError):
            ExecutionModeManager.from_env()

    def test_describe(self):
        assert "模拟" in ExecutionModeManager(ExecutionMode.PAPER).describe()
        assert "真实" in ExecutionModeManager(ExecutionMode.LIVE).describe()

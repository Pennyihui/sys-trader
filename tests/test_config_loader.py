import os
import tempfile
import pytest
from shared.config_loader import load_yaml_config, load_symbols, load_risk_config


class TestConfigLoader:
    def test_load_yaml_config_returns_dict(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("key: value\nlist:\n  - a\n  - b\n")
            f.flush()
            result = load_yaml_config(f.name)
        os.unlink(f.name)
        assert result == {"key": "value", "list": ["a", "b"]}

    def test_load_symbols_merges_primary_and_secondary(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("symbols:\n  primary:\n    - BTCUSDT\n    - ETHUSDT\n  secondary:\n    - BNBUSDT\n    - DOGEUSDT\n")
            f.flush()
            symbols = load_symbols(f.name)
        os.unlink(f.name)
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols
        assert "BNBUSDT" in symbols
        assert "DOGEUSDT" in symbols
        assert len(symbols) == 4

    def test_load_risk_config_returns_correct_defaults(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("risk:\n  risk_per_trade: 0.02\n  max_leverage: 3\n  max_drawdown: 0.10\n  daily_loss_limit: 0.05\n  consecutive_loss_breaker: 3\n  cooldown_minutes: 60\n  max_position_per_symbol: 0.25\n  max_same_direction: 0.40\n  max_total_margin: 0.75\n")
            f.flush()
            config = load_risk_config(f.name)
        os.unlink(f.name)
        assert config.risk_per_trade == 0.02
        assert config.max_leverage == 3
        assert config.max_drawdown == 0.10
        assert config.cooldown_minutes == 60

    def test_load_yaml_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            load_yaml_config("/nonexistent/path.yaml")

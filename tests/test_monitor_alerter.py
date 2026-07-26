import pytest
from unittest.mock import patch, MagicMock
from monitor.alerter import Alerter, AlertLevel, Alert


@pytest.mark.unit
class TestAlerter:
    def setup_method(self):
        self.alerts = []
        self.alerter = Alerter(on_alert=lambda a: self.alerts.append(a))

    def test_fire_critical_alert(self):
        self.alerter.fire(AlertLevel.CRITICAL, "margin_ratio", "Margin ratio at 85%", {"margin_ratio": 0.85})
        assert len(self.alerts) == 1
        assert self.alerts[0].level == AlertLevel.CRITICAL
        assert self.alerts[0].metric == "margin_ratio"
        assert "85%" in self.alerts[0].message

    def test_fire_warning_alert(self):
        self.alerter.fire(AlertLevel.WARNING, "daily_pnl", "Daily PnL approaching limit", {"pnl": -300.0})
        assert len(self.alerts) == 1
        assert self.alerts[0].level == AlertLevel.WARNING

    def test_fire_info_alert(self):
        self.alerter.fire(AlertLevel.INFO, "signal.generated", "Signal generated", {"symbol": "BTCUSDT"})
        assert len(self.alerts) == 1
        assert self.alerts[0].level == AlertLevel.INFO

    def test_check_heartbeat_not_firing_when_recent(self):
        from monitor.collector import MetricsCollector
        MetricsCollector.reset()
        collector = MetricsCollector.instance()
        collector.heartbeat("market_data")
        self.alerter.check_heartbeat("market_data", collector)
        assert len(self.alerts) == 0

    def test_check_heartbeat_fires_when_timeout(self):
        from monitor.collector import MetricsCollector
        import time
        MetricsCollector.reset()
        collector = MetricsCollector.instance()
        collector.heartbeat("market_data")
        collector._heartbeats["market_data"] = time.time() - 120
        self.alerter.check_heartbeat("market_data", collector, timeout_seconds=60)
        assert len(self.alerts) == 1
        assert self.alerts[0].level == AlertLevel.CRITICAL
        assert "market_data" in self.alerts[0].message

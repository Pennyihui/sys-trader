"""soak_watchdog 测试。"""
import pytest

from tools.soak_watchdog import rss_mb, collect_metrics, count_errors


@pytest.mark.unit
class TestSoakWatchdog:
    def test_collect_metrics_shape(self):
        m = collect_metrics()
        assert "rss_mb" in m
        assert "errors_last_hour" in m
        assert "ts" in m
        # psutil 可选：未安装时 rss_mb 回退 0.0，断言只要求非负避免环境依赖
        assert m["rss_mb"] >= 0

    def test_rss_mb_positive_with_psutil(self):
        pytest.importorskip("psutil")
        assert rss_mb() > 0

    def test_count_errors_missing_file(self):
        assert count_errors("no_such_file.log") == 0
        assert count_errors(None) == 0

    def test_count_errors_counts_error_and_warning_lines(self, tmp_path):
        log = tmp_path / "systrader.log"
        log.write_text("INFO ok\nERROR boom\nWARNING caution\nERROR again\n",
                       encoding="utf-8")
        assert count_errors(str(log)) == 3

    def test_collect_metrics_reads_log(self, tmp_path):
        log = tmp_path / "systrader.log"
        log.write_text("INFO ok\nWARNING warn\n", encoding="utf-8")
        m = collect_metrics(str(log))
        assert m["errors_last_hour"] == 1

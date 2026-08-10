"""soak_watchdog 测试。"""
import os

import pytest

from tools.soak_watchdog import rss_mb, collect_metrics, count_errors, sample_and_append


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

    def test_rss_mb_with_pid(self):
        pytest.importorskip("psutil")
        assert rss_mb(os.getpid()) > 0

    def test_rss_mb_invalid_pid_returns_zero(self):
        """不存在的 pid → psutil.NoSuchProcess → 回退 0.0（无 psutil 时同路径）。"""
        assert rss_mb(999999999) == 0.0

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

    def test_csv_row_is_delta_not_cumulative(self, tmp_path):
        """追加行写入的是增量而非累计值（列名 errors_delta）。"""
        out = tmp_path / "soak_metrics.csv"
        log = tmp_path / "systrader.log"
        log.write_text("INFO a\nERROR one\n", encoding="utf-8")
        out.write_text("ts,rss_mb,errors_delta\n", encoding="utf-8")
        last = sample_and_append(str(out), str(log), 0)
        assert last == 1
        log.write_text("INFO a\nERROR one\nWARNING two\nERROR three\n",
                       encoding="utf-8")
        last = sample_and_append(str(out), str(log), last)
        assert last == 3
        lines = out.read_text(encoding="utf-8").strip().splitlines()
        assert lines[0] == "ts,rss_mb,errors_delta"
        assert lines[1].split(",")[2] == "1"
        assert lines[2].split(",")[2] == "2"

"""测试 retrier 装饰器。"""
import pytest
import time
from shared.retry import retrier


class TestRetrier:
    def test_success_first_try(self):
        calls = []
        @retrier(max_retries=3)
        def fn():
            calls.append(1)
            return "ok"
        assert fn() == "ok"
        assert len(calls) == 1

    def test_retries_until_success(self):
        calls = []
        @retrier(max_retries=3, backoff=0.01)
        def fn():
            calls.append(1)
            if len(calls) < 3:
                raise ConnectionError("timeout")
            return "ok"
        assert fn() == "ok"
        assert len(calls) == 3

    def test_gives_up_after_max_retries(self):
        calls = []
        @retrier(max_retries=3, backoff=0.01)
        def fn():
            calls.append(1)
            raise ValueError("boom")
        with pytest.raises(ValueError):
            fn()
        assert len(calls) == 3

    def test_retry_only_specified_exceptions(self):
        calls = []
        @retrier(max_retries=3, backoff=0.01, retry_on=(ConnectionError,))
        def fn():
            calls.append(1)
            raise ValueError("not retryable")
        with pytest.raises(ValueError):
            fn()
        assert len(calls) == 1  # 不重试

    def test_max_retries_zero_runs_once_and_raises_original(self):
        """max_retries=0: 执行一次不重试, 异常原样抛出 (不 raise None)。"""
        calls = []
        @retrier(max_retries=0)
        def fn():
            calls.append(1)
            raise ValueError("boom")
        with pytest.raises(ValueError):
            fn()
        assert len(calls) == 1

    def test_max_retries_negative_runs_once_on_success(self):
        @retrier(max_retries=-1)
        def fn():
            return "ok"
        assert fn() == "ok"

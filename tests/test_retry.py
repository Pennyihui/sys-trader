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

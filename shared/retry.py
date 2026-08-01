"""API 重试装饰器 — 指数退避 + 可选抖动。"""

import logging
import random
import time
from functools import wraps
from typing import Callable, Optional

logger = logging.getLogger(__name__)


def retrier(max_retries: int = 3, backoff: float = 1.0, jitter: float = 0.1,
            retry_on: Optional[tuple] = None):
    """指数退避重试装饰器。

    Args:
        max_retries: 最大重试次数
        backoff: 基础退避秒数 (2^n * backoff)
        jitter: 抖动比例
        retry_on: 需要重试的异常类型元组，默认 (Exception,)
    """
    retry_on = retry_on or (Exception,)

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    last_error = e
                    if attempt == max_retries - 1:
                        break
                    delay = (2 ** attempt) * backoff
                    if jitter > 0:
                        delay *= (1 + random.uniform(-jitter, jitter))
                    logger.warning("Retry %s (attempt %d/%d) after %.1fs: %s",
                                   func.__name__, attempt + 1, max_retries, delay, e)
                    time.sleep(delay)
            raise last_error
        return wrapper
    return decorator

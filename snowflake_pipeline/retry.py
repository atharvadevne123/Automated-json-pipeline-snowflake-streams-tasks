"""Exponential-backoff retry decorator for the pipeline."""
from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Callable, Type

from snowflake_pipeline.constants import DEFAULT_RETRY_ATTEMPTS, DEFAULT_RETRY_BASE_DELAY, DEFAULT_RETRY_MAX_DELAY
from snowflake_pipeline.exceptions import RetryExhausted

logger = logging.getLogger(__name__)


def retry(
    attempts: int = DEFAULT_RETRY_ATTEMPTS,
    base_delay: float = DEFAULT_RETRY_BASE_DELAY,
    max_delay: float = DEFAULT_RETRY_MAX_DELAY,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """Decorator that retries the wrapped function on failure.

    Args:
        attempts: Maximum number of total attempts (>= 1).
        base_delay: Initial backoff in seconds (doubles each retry).
        max_delay: Upper bound on backoff delay.
        exceptions: Tuple of exception types to catch and retry.

    Returns:
        Decorator function.

    Raises:
        RetryExhausted: after all attempts are exhausted.
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exc: Exception = RuntimeError("No attempts made")
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == attempts:
                        break
                    logger.warning(
                        "Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                        attempt, attempts, fn.__name__, exc, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
            raise RetryExhausted(attempts, last_exc)
        return wrapper
    return decorator


def retry_call(
    fn: Callable,
    *args,
    attempts: int = DEFAULT_RETRY_ATTEMPTS,
    base_delay: float = DEFAULT_RETRY_BASE_DELAY,
    max_delay: float = DEFAULT_RETRY_MAX_DELAY,
    **kwargs,
):
    """Call *fn* with retry logic without using the decorator.

    Args:
        fn: Callable to invoke.
        *args: Positional arguments for fn.
        attempts: Maximum attempts.
        base_delay: Initial backoff seconds.
        max_delay: Maximum backoff seconds.
        **kwargs: Keyword arguments for fn.

    Returns:
        Return value of fn on success.

    Raises:
        RetryExhausted: after all attempts are exhausted.
    """
    return retry(attempts=attempts, base_delay=base_delay, max_delay=max_delay)(fn)(*args, **kwargs)

"""Tests for snowflake_pipeline.retry."""
from __future__ import annotations

import pytest

from snowflake_pipeline.exceptions import RetryExhausted
from snowflake_pipeline.retry import retry, retry_call


def test_retry_succeeds_first_attempt():
    calls = []

    @retry(attempts=3, base_delay=0)
    def fn():
        calls.append(1)
        return "ok"

    assert fn() == "ok"
    assert len(calls) == 1


def test_retry_retries_on_failure():
    calls = []

    @retry(attempts=3, base_delay=0)
    def fn():
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("not ready")
        return "done"

    assert fn() == "done"
    assert len(calls) == 3


def test_retry_exhausted_raises():
    @retry(attempts=2, base_delay=0)
    def always_fail():
        raise RuntimeError("boom")

    with pytest.raises(RetryExhausted) as exc_info:
        always_fail()
    assert exc_info.value.attempts == 2
    assert isinstance(exc_info.value.last_error, RuntimeError)


def test_retry_only_catches_specified_exceptions():
    @retry(attempts=3, base_delay=0, exceptions=(ValueError,))
    def fn():
        raise TypeError("different exception")

    with pytest.raises(TypeError):
        fn()


def test_retry_call_succeeds():
    def add(a, b):
        return a + b

    result = retry_call(add, 2, 3, attempts=2, base_delay=0)
    assert result == 5


def test_retry_call_exhausted():
    def always_fail():
        raise OSError("disk error")

    with pytest.raises(RetryExhausted):
        retry_call(always_fail, attempts=2, base_delay=0)


def test_retry_exhausted_str():
    exc = RetryExhausted(3, ValueError("inner"))
    assert "3" in str(exc)


def test_retry_preserves_return_value_with_kwargs():
    @retry(attempts=2, base_delay=0)
    def fn(x, *, multiplier=1):
        return x * multiplier

    assert fn(4, multiplier=3) == 12


# ---------------------------------------------------------------------------
# jitter parameter
# ---------------------------------------------------------------------------

def test_retry_with_jitter_succeeds():
    from snowflake_pipeline.retry import retry
    call_count = 0

    @retry(attempts=3, base_delay=0.001, jitter=True)
    def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("not yet")
        return "ok"

    result = flaky()
    assert result == "ok"
    assert call_count == 2


def test_retry_without_jitter_is_deterministic():
    from snowflake_pipeline.retry import retry
    from snowflake_pipeline.exceptions import RetryExhausted
    attempts_made = []

    @retry(attempts=2, base_delay=0.001, jitter=False)
    def always_fails():
        attempts_made.append(1)
        raise RuntimeError("always")

    with pytest.raises(RetryExhausted):
        always_fails()
    assert len(attempts_made) == 2

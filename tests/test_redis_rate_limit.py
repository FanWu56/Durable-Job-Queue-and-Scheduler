import pytest

from app import rate_limit


def test_redis_rate_limit_blocks_after_limit():
    try:
        rate_limit.redis_client.ping()
    except Exception:
        pytest.skip("Redis is not running.")

    rate_limit.RATE_LIMITS["send_email"] = 2

    keys = rate_limit.redis_client.keys("rate_limit:send_email:*")

    if keys:
        rate_limit.redis_client.delete(*keys)

    first = rate_limit.is_rate_limited("send_email")
    second = rate_limit.is_rate_limited("send_email")
    third = rate_limit.is_rate_limited("send_email")

    assert first is False
    assert second is False
    assert third is True
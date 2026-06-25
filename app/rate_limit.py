from datetime import datetime, timezone

import redis

from .config import REDIS_URL


RATE_LIMITS = {
    "send_email": 5,
}


redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
)


def is_rate_limited(task_name: str) -> bool:
    limit = RATE_LIMITS.get(task_name)

    if limit is None:
        print(f"[rate-limit] no limit for task={task_name}")
        return False

    now = datetime.now(timezone.utc)
    minute_bucket = now.strftime("%Y%m%d%H%M")

    key = f"rate_limit:{task_name}:{minute_bucket}"

    count = redis_client.incr(key)

    if count == 1:
        redis_client.expire(key, 70)

    print(f"[rate-limit] task={task_name} count={count} limit={limit} key={key}")

    return count > limit
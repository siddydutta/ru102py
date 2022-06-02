from time import time
from random import randint
from redis.client import Redis
from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        key = self.key_schema.sliding_window_rate_limiter_key(name, self.window_size_ms, self.max_hits)
        pipeline = self.redis.pipeline()

        current_timestamp = time() * 1000
        value = f'{current_timestamp}-{randint(100, 999)}'
        mapping = {value: current_timestamp}

        pipeline.zadd(key, mapping)
        pipeline.zremrangebyscore(key, 0, current_timestamp-self.window_size_ms)
        pipeline.zcard(key)
        _, _, hits = pipeline.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException()

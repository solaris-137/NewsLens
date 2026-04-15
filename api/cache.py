import json
import os

import redis.asyncio as aioredis

redis_client = aioredis.from_url(os.environ["REDIS_URL"], decode_responses=True)


async def cache_get(key: str):
    value = await redis_client.get(key)
    return json.loads(value) if value else None


async def cache_set(key: str, value, ttl_seconds: int):
    await redis_client.setex(key, ttl_seconds, json.dumps(value))


async def cache_delete(key: str):
    await redis_client.delete(key)

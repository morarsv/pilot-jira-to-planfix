from redis.asyncio import Redis
from contextlib import asynccontextmanager


@asynccontextmanager
async def redis_client(**kwargs):
    r = Redis(**kwargs)
    try:
        yield r
    finally:
        await r.aclose()

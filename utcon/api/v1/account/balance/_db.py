# _db.py
import os
import asyncio
import asyncpg

DB_DSN = os.getenv("UTDB_DSN", "postgresql://user:password@10.1.0.91:9000/utdb")

_pool: asyncpg.pool.Pool | None = None

async def get_pool() -> asyncpg.pool.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=10)
    return _pool

async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
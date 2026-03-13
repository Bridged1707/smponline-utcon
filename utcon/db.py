import os
import asyncpg
from contextlib import asynccontextmanager

_pool = None


def build_dsn():
    return (
        f"postgresql://{os.getenv('UTDB_USER')}:"
        f"{os.getenv('UTDB_PASSWORD')}@"
        f"{os.getenv('UTDB_HOST')}:"
        f"{os.getenv('UTDB_PORT')}/"
        f"{os.getenv('UTDB_NAME')}"
    )


async def connect():
    global _pool

    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=build_dsn(),
            min_size=2,
            max_size=20
        )


async def close():
    global _pool

    if _pool:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def connection():
    conn = await _pool.acquire()
    try:
        yield conn
    finally:
        await _pool.release(conn)
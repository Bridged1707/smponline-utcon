import os
import asyncpg
from contextlib import asynccontextmanager

pool = None


async def connect():
    global pool

    pool = await asyncpg.create_pool(
        host=os.getenv("UTDB_HOST"),
        port=int(os.getenv("UTDB_PORT", 5432)),
        user=os.getenv("UTDB_USER"),
        password=os.getenv("UTDB_PASSWORD"),
        database=os.getenv("UTDB_NAME"),
        min_size=1,
        max_size=10
    )


async def disconnect():
    global pool
    if pool:
        await pool.close()


def get_pool():
    return pool


@asynccontextmanager
async def connection():
    conn = await pool.acquire()
    try:
        yield conn
    finally:
        await pool.release(conn)
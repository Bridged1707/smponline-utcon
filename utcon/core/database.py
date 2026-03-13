import asyncpg
import os

DB_HOST = os.getenv("DB_HOST", "10.1.0.91")
DB_PORT = os.getenv("DB_PORT", "9000")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")

pool = None


async def get_pool():
    global pool

    if pool is None:
        pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )

    return pool
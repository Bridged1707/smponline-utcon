import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

_pool = None


async def connect():

    global _pool

    host = os.getenv("UTDB_HOST")
    port = os.getenv("UTDB_PORT")
    user = os.getenv("UTDB_USER")
    password = os.getenv("UTDB_PASSWORD")
    database = os.getenv("UTDB_NAME")

    if not port:
        raise RuntimeError("UTDB_PORT not set in .env")

    _pool = await asyncpg.create_pool(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database,
        min_size=2,
        max_size=10,
    )


async def close():
    await _pool.close()


class connection:

    async def __aenter__(self):
        self.conn = await _pool.acquire()
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        await _pool.release(self.conn)
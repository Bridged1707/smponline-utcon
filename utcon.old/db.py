import os
import asyncpg
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pathlib import Path

# force load .env from project root
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

_pool = None


def build_dsn():
    host = os.getenv("UTDB_HOST")
    port = os.getenv("UTDB_PORT")
    name = os.getenv("UTDB_NAME")
    user = os.getenv("UTDB_USER")
    password = os.getenv("UTDB_PASSWORD")

    if not host:
        raise RuntimeError("UTDB_HOST missing")
    if not port:
        raise RuntimeError("UTDB_PORT missing")
    if not name:
        raise RuntimeError("UTDB_NAME missing")
    if not user:
        raise RuntimeError("UTDB_USER missing")
    if not password:
        raise RuntimeError("UTDB_PASSWORD missing")

    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


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
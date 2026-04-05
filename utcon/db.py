import os
import asyncio
import asyncpg
from contextlib import asynccontextmanager

pool = None
replica_pool = None
_replication_task = None
_replication_lock = asyncio.Lock()


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _replica_enabled() -> bool:
    return os.getenv("UTDB_REPLICA_ENABLED", "true").lower() in {"1", "true", "yes", "on"}


def _has_replica_config() -> bool:
    return bool(os.getenv("UTDB_REPLICA_USER") and os.getenv("UTDB_REPLICA_PASSWORD") and os.getenv("UTDB_REPLICA_NAME"))


def _replica_sync_on_start_enabled() -> bool:
    return os.getenv("UTDB_REPLICA_SYNC_ON_START", "true").lower() in {"1", "true", "yes", "on"}


async def connect():
    global pool, replica_pool

    pool = await asyncpg.create_pool(
        host=os.getenv("UTDB_HOST"),
        port=int(os.getenv("UTDB_PORT", 5432)),
        user=os.getenv("UTDB_USER"),
        password=os.getenv("UTDB_PASSWORD"),
        database=os.getenv("UTDB_NAME"),
        min_size=1,
        max_size=10
    )

    if _replica_enabled() and _has_replica_config():
        replica_pool = await asyncpg.create_pool(
            host=os.getenv("UTDB_REPLICA_HOST", "68.37.88.100"),
            port=int(os.getenv("UTDB_REPLICA_PORT", 8888)),
            user=os.getenv("UTDB_REPLICA_USER"),
            password=os.getenv("UTDB_REPLICA_PASSWORD"),
            database=os.getenv("UTDB_REPLICA_NAME"),
            min_size=1,
            max_size=3,
        )
        print("[UTCON] remote replica enabled")

        if _replica_sync_on_start_enabled():
            print("[UTCON] starting initial full replica sync...")
            try:
                await _copy_full_database_to_replica()
                print("[UTCON] initial full replica sync complete")
            except Exception as exc:
                print(f"[UTCON] initial full replica sync failed: {exc}")
    else:
        replica_pool = None
        print("[UTCON] remote replica disabled (missing config or disabled flag)")


async def disconnect():
    global pool, replica_pool
    if pool:
        await pool.close()
    if replica_pool:
        await replica_pool.close()


def get_pool():
    return pool


def get_replica_pool():
    return replica_pool


async def _copy_full_database_to_replica() -> None:
    if pool is None or replica_pool is None:
        return

    async with _replication_lock:
        async with pool.acquire() as src_conn, replica_pool.acquire() as dst_conn:
            tables = await src_conn.fetch(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
                """
            )

            async with dst_conn.transaction():
                for table in tables:
                    table_schema = table["table_schema"]
                    table_name = table["table_name"]
                    await dst_conn.execute(
                        f"TRUNCATE TABLE {_quote_ident(table_schema)}.{_quote_ident(table_name)} RESTART IDENTITY CASCADE"
                    )

                for table in tables:
                    table_schema = table["table_schema"]
                    table_name = table["table_name"]

                    rows = await src_conn.fetch(
                        f"SELECT * FROM {_quote_ident(table_schema)}.{_quote_ident(table_name)}"
                    )

                    if not rows:
                        continue

                    columns = list(rows[0].keys())
                    col_sql = ", ".join(_quote_ident(col) for col in columns)
                    placeholders = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
                    insert_sql = (
                        f"INSERT INTO {_quote_ident(table_schema)}.{_quote_ident(table_name)} "
                        f"({col_sql}) VALUES ({placeholders})"
                    )

                    values = [tuple(row[col] for col in columns) for row in rows]
                    await dst_conn.executemany(insert_sql, values)


def _schedule_replication() -> None:
    global _replication_task

    if replica_pool is None:
        return

    if _replication_task and not _replication_task.done():
        return

    async def _runner():
        try:
            await _copy_full_database_to_replica()
        except Exception as exc:
            print(f"[UTCON] remote replication failed: {exc}")

    _replication_task = asyncio.create_task(_runner())


@asynccontextmanager
async def connection():
    conn = await pool.acquire()
    before_wal_lsn = None
    committed = False

    try:
        if replica_pool is not None:
            before_wal_lsn = await conn.fetchval("SELECT pg_current_wal_lsn()")

        async with conn.transaction():
            yield conn

        committed = True

        if replica_pool is not None and before_wal_lsn is not None:
            after_wal_lsn = await conn.fetchval("SELECT pg_current_wal_lsn()")
            if after_wal_lsn != before_wal_lsn:
                _schedule_replication()
    finally:
        await pool.release(conn)
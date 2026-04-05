from __future__ import annotations

from typing import Any, Dict, Optional


async def ensure_schema(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS casino_users (
            discord_uuid TEXT PRIMARY KEY,
            sender_external_id TEXT NOT NULL,
            balance BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS casino_pf_params (
            discord_uuid TEXT PRIMARY KEY,
            client_seed TEXT NOT NULL,
            server_seed TEXT NOT NULL,
            nonce BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS casino_financial_transactions (
            id BIGSERIAL PRIMARY KEY,
            discord_uuid TEXT NOT NULL,
            type TEXT NOT NULL,
            amount BIGINT NOT NULL,
            net_amount BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS casino_state (
            state_key TEXT PRIMARY KEY,
            message_id BIGINT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS casino_tables (
            channel_id BIGINT PRIMARY KEY,
            category_id BIGINT NOT NULL,
            table_number INTEGER NOT NULL,
            channel_name TEXT NOT NULL,
            category_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_casino_tables_table_number
            ON casino_tables(table_number);

        CREATE INDEX IF NOT EXISTS idx_casino_financial_transactions_discord_uuid
            ON casino_financial_transactions(discord_uuid);
        """
    )


def _row_to_dict(row) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


def _extract_affected_count(status: str | None) -> int:
    if not status:
        return 0
    parts = status.strip().split()
    if not parts:
        return 0
    try:
        return int(parts[-1])
    except ValueError:
        return 0


async def get_user(conn, *, discord_uuid: str) -> Optional[Dict[str, Any]]:
    await ensure_schema(conn)
    row = await conn.fetchrow(
        """
        SELECT discord_uuid, sender_external_id, balance, created_at, updated_at
        FROM casino_users
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )
    return _row_to_dict(row)


async def register_user(conn, *, discord_uuid: str, sender_external_id: str) -> Dict[str, Any]:
    await ensure_schema(conn)
    row = await conn.fetchrow(
        """
        INSERT INTO casino_users(discord_uuid, sender_external_id)
        VALUES($1, $2)
        ON CONFLICT (discord_uuid)
        DO UPDATE SET
            sender_external_id = EXCLUDED.sender_external_id,
            updated_at = NOW()
        RETURNING discord_uuid, sender_external_id, balance, created_at, updated_at
        """,
        discord_uuid,
        sender_external_id,
    )
    return dict(row)


async def update_user_balance(conn, *, discord_uuid: str, amount_delta: int) -> Dict[str, Any]:
    await ensure_schema(conn)
    row = await conn.fetchrow(
        """
        UPDATE casino_users
        SET balance = balance + $2,
            updated_at = NOW()
        WHERE discord_uuid = $1
        RETURNING discord_uuid, sender_external_id, balance, created_at, updated_at
        """,
        discord_uuid,
        amount_delta,
    )
    if row is None:
        raise LookupError("casino_user_not_found")
    return dict(row)


async def get_pf_params(conn, *, discord_uuid: str) -> Optional[Dict[str, Any]]:
    await ensure_schema(conn)
    row = await conn.fetchrow(
        """
        SELECT discord_uuid, client_seed, server_seed, nonce, created_at, updated_at
        FROM casino_pf_params
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )
    return _row_to_dict(row)


async def save_pf_params(
    conn,
    *,
    discord_uuid: str,
    client_seed: str,
    server_seed: str,
    nonce: int,
) -> Dict[str, Any]:
    await ensure_schema(conn)
    row = await conn.fetchrow(
        """
        INSERT INTO casino_pf_params(discord_uuid, client_seed, server_seed, nonce)
        VALUES($1, $2, $3, $4)
        ON CONFLICT (discord_uuid)
        DO UPDATE SET
            client_seed = EXCLUDED.client_seed,
            server_seed = EXCLUDED.server_seed,
            nonce = EXCLUDED.nonce,
            updated_at = NOW()
        RETURNING discord_uuid, client_seed, server_seed, nonce, created_at, updated_at
        """,
        discord_uuid,
        client_seed,
        server_seed,
        nonce,
    )
    return dict(row)


async def append_financial_transaction(
    conn,
    *,
    discord_uuid: str,
    transaction_type: str,
    amount: int,
    net_amount: int,
) -> Dict[str, Any]:
    await ensure_schema(conn)

    exists = await conn.fetchval(
        """
        SELECT 1
        FROM casino_users
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )
    if exists is None:
        raise LookupError("casino_user_not_found")

    row = await conn.fetchrow(
        """
        INSERT INTO casino_financial_transactions(discord_uuid, type, amount, net_amount)
        VALUES($1, $2, $3, $4)
        RETURNING id, discord_uuid, type, amount, net_amount, created_at
        """,
        discord_uuid,
        transaction_type,
        amount,
        net_amount,
    )
    return dict(row)


async def save_account_panel_message(conn, *, message_id: int) -> Dict[str, Any]:
    await ensure_schema(conn)
    row = await conn.fetchrow(
        """
        INSERT INTO casino_state(state_key, message_id)
        VALUES('account_panel', $1)
        ON CONFLICT (state_key)
        DO UPDATE SET
            message_id = EXCLUDED.message_id,
            updated_at = NOW()
        RETURNING state_key, message_id, updated_at
        """,
        message_id,
    )
    return dict(row)


async def get_account_panel_message(conn) -> Optional[Dict[str, Any]]:
    await ensure_schema(conn)
    row = await conn.fetchrow(
        """
        SELECT state_key, message_id, updated_at
        FROM casino_state
        WHERE state_key = 'account_panel'
        """
    )
    return _row_to_dict(row)


async def create_table(
    conn,
    *,
    channel_id: int,
    category_id: int,
    table_number: int,
    channel_name: str,
    category_name: str,
) -> Dict[str, Any]:
    await ensure_schema(conn)
    row = await conn.fetchrow(
        """
        INSERT INTO casino_tables(channel_id, category_id, table_number, channel_name, category_name)
        VALUES($1, $2, $3, $4, $5)
        ON CONFLICT (channel_id)
        DO UPDATE SET
            category_id = EXCLUDED.category_id,
            table_number = EXCLUDED.table_number,
            channel_name = EXCLUDED.channel_name,
            category_name = EXCLUDED.category_name,
            updated_at = NOW()
        RETURNING channel_id, category_id, table_number, channel_name, category_name, created_at, updated_at
        """,
        channel_id,
        category_id,
        table_number,
        channel_name,
        category_name,
    )
    return dict(row)


async def list_tables(conn) -> list[Dict[str, Any]]:
    await ensure_schema(conn)
    rows = await conn.fetch(
        """
        SELECT channel_id, category_id, table_number, channel_name, category_name, created_at, updated_at
        FROM casino_tables
        ORDER BY table_number ASC, channel_id ASC
        """
    )
    return [dict(row) for row in rows]


async def delete_table(conn, *, channel_id: int) -> bool:
    await ensure_schema(conn)
    status = await conn.execute(
        """
        DELETE FROM casino_tables
        WHERE channel_id = $1
        """,
        channel_id,
    )
    return _extract_affected_count(status) > 0


async def clear_tables(conn) -> int:
    await ensure_schema(conn)
    status = await conn.execute("DELETE FROM casino_tables")
    return _extract_affected_count(status)


async def count_tables(conn) -> int:
    await ensure_schema(conn)
    count = await conn.fetchval("SELECT COUNT(*) FROM casino_tables")
    return int(count or 0)

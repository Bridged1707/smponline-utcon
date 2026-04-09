from __future__ import annotations

from typing import Any, Dict, Optional
import json
from decimal import Decimal, ROUND_HALF_UP

import asyncpg

from utcon.repositories import membership as membership_repo
from utcon.repositories import balance as balance_repo


BPS_DENOMINATOR = Decimal("10000")
PAYOUT_QUANTIZE = Decimal("0.00000001")
CASINO_FEE_RATE_BPS_BY_TIER = {"free": 1000, "pro": 700, "garry": 300}


def _to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    return Decimal(str(value))


def _normalize_tier(value: Any) -> str:
    tier = str(value or "").strip().lower()
    return tier if tier in CASINO_FEE_RATE_BPS_BY_TIER else "free"


def _quantize(value: Decimal) -> Decimal:
    return _to_decimal(value).quantize(PAYOUT_QUANTIZE, rounding=ROUND_HALF_UP)


async def _get_fee_profile(conn, discord_uuid: str, requested_tier: Any | None = None) -> tuple[str, int]:
    explicit_tier = str(requested_tier or "").strip().lower()
    if explicit_tier in CASINO_FEE_RATE_BPS_BY_TIER:
        return explicit_tier, CASINO_FEE_RATE_BPS_BY_TIER[explicit_tier]

    membership = await membership_repo.get_effective_membership(conn, discord_uuid)
    tier = _normalize_tier((membership or {}).get("tier"))
    return tier, CASINO_FEE_RATE_BPS_BY_TIER[tier]


def _calculate_settlement(*, wager_amount: Decimal, gross_payout_amount: Decimal, fee_rate_bps: int) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    wager_amount = _to_decimal(wager_amount)
    gross_payout_amount = _to_decimal(gross_payout_amount)

    if gross_payout_amount <= Decimal("0"):
        return Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")

    gross_profit_amount = gross_payout_amount - wager_amount
    if gross_profit_amount <= Decimal("0"):
        return gross_payout_amount, Decimal("0"), gross_payout_amount, gross_payout_amount - wager_amount

    fee_amount = _quantize(gross_profit_amount * Decimal(int(fee_rate_bps)) / BPS_DENOMINATOR)
    if fee_amount < Decimal("0"):
        fee_amount = Decimal("0")
    if fee_amount > gross_profit_amount:
        fee_amount = gross_profit_amount

    net_payout_amount = gross_payout_amount - fee_amount
    net_profit_amount = net_payout_amount - wager_amount
    return gross_payout_amount, fee_amount, net_payout_amount, net_profit_amount


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

        CREATE TABLE IF NOT EXISTS casino_game_sessions (
            id BIGSERIAL PRIMARY KEY,
            discord_uuid TEXT NOT NULL,
            game_type TEXT NOT NULL,
            wager_amount NUMERIC NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            outcome TEXT,
            membership_tier TEXT,
            fee_rate_bps INTEGER NOT NULL DEFAULT 0,
            gross_payout_amount NUMERIC NOT NULL DEFAULT 0,
            fee_amount NUMERIC NOT NULL DEFAULT 0,
            net_payout_amount NUMERIC NOT NULL DEFAULT 0,
            profit_amount NUMERIC NOT NULL DEFAULT 0,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at TIMESTAMPTZ
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

        CREATE INDEX IF NOT EXISTS idx_casino_game_sessions_discord_uuid
            ON casino_game_sessions(discord_uuid, created_at DESC);
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


async def _insert_balance_transaction_compat(
    conn,
    *,
    discord_uuid: str,
    preferred_kind: str,
    amount: Decimal,
    metadata: Dict[str, Any] | None = None,
) -> str:
    """
    Some deployed UTDB instances still enforce older CHECK constraints on
    balance_transactions.kind. Prefer the semantic gambling kind, but fall back
    to admin_add with the signed amount so gameplay does not 500.
    """
    try:
        await balance_repo.insert_balance_transaction(
            conn,
            discord_uuid=discord_uuid,
            kind=preferred_kind,
            amount=amount,
            metadata=metadata,
        )
        return preferred_kind
    except asyncpg.PostgresError:
        fallback_metadata = dict(metadata or {})
        fallback_metadata.setdefault("original_kind", preferred_kind)
        fallback_metadata.setdefault("compat_fallback", "admin_add")
        await balance_repo.insert_balance_transaction(
            conn,
            discord_uuid=discord_uuid,
            kind="admin_add",
            amount=amount,
            metadata=fallback_metadata,
        )
        return "admin_add"


async def get_user(conn, *, discord_uuid: str) -> Optional[Dict[str, Any]]:
    casino_user: Optional[Dict[str, Any]] = None

    try:
        row = await conn.fetchrow(
            """
            SELECT discord_uuid, sender_external_id, balance, created_at, updated_at
            FROM casino_users
            WHERE discord_uuid = $1
            """,
            discord_uuid,
        )
        casino_user = _row_to_dict(row)
    except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError):
        casino_user = None

    balance_row = await conn.fetchrow(
        """
        SELECT discord_uuid, balance
        FROM balances
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )

    if balance_row is None and casino_user is None:
        return None

    if balance_row is None:
        return casino_user

    core_balance = balance_row["balance"]

    if casino_user is None:
        return {
            "discord_uuid": discord_uuid,
            "sender_external_id": discord_uuid,
            "balance": core_balance,
            "created_at": None,
            "updated_at": None,
        }

    casino_user["balance"] = core_balance
    return casino_user


async def register_user(conn, *, discord_uuid: str, sender_external_id: str) -> Dict[str, Any]:
    await conn.execute(
        """
        INSERT INTO balances(discord_uuid, balance)
        VALUES($1, 0)
        ON CONFLICT (discord_uuid) DO NOTHING
        """,
        discord_uuid,
    )

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
    balance_row = await conn.fetchrow(
        """
        UPDATE balances
        SET balance = balance + $2,
            last_updated = NOW()
        WHERE discord_uuid = $1
        RETURNING discord_uuid, balance, last_updated
        """,
        discord_uuid,
        amount_delta,
    )
    if balance_row is None:
        raise LookupError("casino_user_not_found")

    # Mirror into casino_users when available; keep this non-fatal to avoid
    # failing gameplay if the optional casino table is unavailable.
    try:
        await conn.execute(
            """
            INSERT INTO casino_users(discord_uuid, sender_external_id, balance)
            VALUES($1, $1, $2)
            ON CONFLICT (discord_uuid)
            DO UPDATE SET
                balance = EXCLUDED.balance,
                updated_at = NOW()
            """,
            discord_uuid,
            balance_row["balance"],
        )
    except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError):
        pass

    return {
        "discord_uuid": balance_row["discord_uuid"],
        "sender_external_id": discord_uuid,
        "balance": balance_row["balance"],
        "updated_at": balance_row["last_updated"],
    }


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


async def start_game_session(
    conn,
    *,
    discord_uuid: str,
    game_type: str,
    wager_amount: Any,
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    await ensure_schema(conn)

    wager_amount = _to_decimal(wager_amount)
    if wager_amount <= Decimal("0"):
        raise ValueError("invalid_wager_amount")

    balance_row = await conn.fetchrow(
        """
        SELECT discord_uuid, balance
        FROM balances
        WHERE discord_uuid = $1
        FOR UPDATE
        """,
        discord_uuid,
    )
    if balance_row is None:
        raise LookupError("casino_user_not_found")

    current_balance = _to_decimal(balance_row["balance"])
    if current_balance < wager_amount:
        raise ValueError("insufficient_balance")

    await conn.execute(
        """
        UPDATE balances
        SET balance = balance - $2,
            last_updated = NOW()
        WHERE discord_uuid = $1
        """,
        discord_uuid,
        wager_amount,
    )

    session = await conn.fetchrow(
        """
        INSERT INTO casino_game_sessions(discord_uuid, game_type, wager_amount, metadata)
        VALUES ($1, $2, $3, $4::jsonb)
        RETURNING *
        """,
        discord_uuid,
        str(game_type).strip().lower(),
        wager_amount,
        json.dumps(metadata or {}),
    )

    balance_tx_metadata = {"game_type": str(game_type).strip().lower(), "session_id": session["id"], **(metadata or {})}
    applied_balance_kind = await _insert_balance_transaction_compat(
        conn,
        discord_uuid=discord_uuid,
        preferred_kind="gambling_wager",
        amount=-wager_amount,
        metadata=balance_tx_metadata,
    )

    try:
        await conn.execute(
            """
            INSERT INTO casino_financial_transactions(discord_uuid, type, amount, net_amount)
            VALUES ($1, 'gambling_wager', $2, $3)
            """,
            discord_uuid,
            int(wager_amount),
            -int(wager_amount),
        )
    except Exception:
        pass

    new_balance = await conn.fetchval("SELECT balance FROM balances WHERE discord_uuid = $1", discord_uuid)
    payload = dict(session)
    payload["current_balance"] = new_balance
    payload["applied_balance_transaction_kind"] = applied_balance_kind
    return {"session": payload, "balance": new_balance, "current_balance": new_balance, "applied_balance_transaction_kind": applied_balance_kind}


async def settle_game_session(
    conn,
    *,
    session_id: int,
    gross_payout_amount: Any,
    outcome: str,
    metadata: Dict[str, Any] | None = None,
    requested_tier: Any | None = None,
) -> Dict[str, Any]:
    await ensure_schema(conn)

    session_row = await conn.fetchrow(
        """
        SELECT *
        FROM casino_game_sessions
        WHERE id = $1
        FOR UPDATE
        """,
        session_id,
    )
    if session_row is None:
        raise LookupError("casino_game_session_not_found")
    if str(session_row["status"] or "").lower() != "open":
        raise RuntimeError("casino_game_session_already_settled")

    discord_uuid = session_row["discord_uuid"]
    balance_row = await conn.fetchrow(
        """
        SELECT balance
        FROM balances
        WHERE discord_uuid = $1
        FOR UPDATE
        """,
        discord_uuid,
    )
    if balance_row is None:
        raise LookupError("casino_user_not_found")

    wager_amount = _to_decimal(session_row["wager_amount"])
    requested_gross_payout = _to_decimal(gross_payout_amount)
    if requested_gross_payout < Decimal("0"):
        raise ValueError("invalid_gross_payout_amount")

    membership_tier, fee_rate_bps = await _get_fee_profile(conn, discord_uuid, requested_tier=requested_tier)
    gross_payout_amount, fee_amount, net_payout_amount, net_profit_amount = _calculate_settlement(
        wager_amount=wager_amount,
        gross_payout_amount=requested_gross_payout,
        fee_rate_bps=fee_rate_bps,
    )

    if net_payout_amount > Decimal("0"):
        await conn.execute(
            """
            UPDATE balances
            SET balance = balance + $2,
                last_updated = NOW()
            WHERE discord_uuid = $1
            """,
            discord_uuid,
            net_payout_amount,
        )

    merged_metadata = dict(session_row["metadata"] or {})
    merged_metadata.update(metadata or {})
    merged_metadata.update({
        "session_id": session_id,
        "game_type": session_row["game_type"],
        "outcome": outcome,
        "gross_payout_amount": str(gross_payout_amount),
        "fee_amount": str(fee_amount),
        "net_payout_amount": str(net_payout_amount),
    })

    updated = await conn.fetchrow(
        """
        UPDATE casino_game_sessions
        SET status = 'settled',
            outcome = $2,
            membership_tier = $3,
            fee_rate_bps = $4,
            gross_payout_amount = $5,
            fee_amount = $6,
            net_payout_amount = $7,
            profit_amount = $8,
            metadata = $9::jsonb,
            resolved_at = NOW()
        WHERE id = $1
        RETURNING *
        """,
        session_id,
        str(outcome).strip().lower(),
        membership_tier,
        fee_rate_bps,
        gross_payout_amount,
        fee_amount,
        net_payout_amount,
        net_profit_amount,
        json.dumps(merged_metadata),
    )

    applied_balance_kind = None
    if net_payout_amount > Decimal("0"):
        applied_balance_kind = await _insert_balance_transaction_compat(
            conn,
            discord_uuid=discord_uuid,
            preferred_kind="gambling_payout",
            amount=net_payout_amount,
            metadata=merged_metadata,
        )

    try:
        await conn.execute(
            """
            INSERT INTO casino_financial_transactions(discord_uuid, type, amount, net_amount)
            VALUES ($1, 'gambling_settlement', $2, $3)
            """,
            discord_uuid,
            int(gross_payout_amount),
            int(net_payout_amount),
        )
    except Exception:
        pass

    new_balance = await conn.fetchval("SELECT balance FROM balances WHERE discord_uuid = $1", discord_uuid)
    payload = dict(updated)
    payload["current_balance"] = new_balance
    if applied_balance_kind is not None:
        payload["applied_balance_transaction_kind"] = applied_balance_kind
    return {"session": payload, "balance": new_balance, "current_balance": new_balance, "applied_balance_transaction_kind": applied_balance_kind}

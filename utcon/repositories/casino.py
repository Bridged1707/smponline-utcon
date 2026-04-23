from __future__ import annotations

from typing import Any, Dict, Optional
import json
import logging
from decimal import Decimal, ROUND_HALF_UP

import asyncpg

from utcon.repositories import balance as balance_repo


BPS_DENOMINATOR = Decimal("10000")
PAYOUT_QUANTIZE = Decimal("0.00000001")
DEFAULT_FEE_RATE_BPS = 1000

logger = logging.getLogger(__name__)

ROLE_FEE_RATE_BPS = {
    "1482895262543515810": 1000,  # Free Tier - 10%
    "1482894699340501114": 700,   # Pro Tier - 7%
    "1482894700749918341": 300,   # Garry Tier - 3%
}

ROLE_WAGER_CAPS = {
    "1482895262543515810": Decimal("16"),
    "1482894699340501114": Decimal("24"),
    "1482894700749918341": Decimal("32"),
}

TIER_WAGER_CAPS = {
    "free": Decimal("16"),
    "pro": Decimal("24"),
    "garry": Decimal("32"),
}

DEFAULT_REGISTERED_WAGER_CAP = TIER_WAGER_CAPS["free"]

ADMIN_ROLE_IDS = {
    # Add hardcoded bypass role IDs here if needed.
    # Prefer DB rows in discord_role_fee_config with fee_rate_bps = 0.
}


def _to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    return Decimal(str(value))


def _quantize(value: Decimal) -> Decimal:
    return _to_decimal(value).quantize(PAYOUT_QUANTIZE, rounding=ROUND_HALF_UP)


def _coerce_metadata_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}

    if isinstance(value, dict):
        return dict(value)

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            decoded = json.loads(raw)
        except Exception:
            return {"_raw_metadata": value}
        if isinstance(decoded, dict):
            return decoded
        return {"_raw_metadata": decoded}

    try:
        return dict(value)
    except Exception:
        return {"_raw_metadata": value}


def _normalize_role_ids(value: Any) -> list[str]:
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        result: list[str] = []
        for entry in value:
            text = str(entry).strip().strip('"').strip("'")
            if text:
                result.append(text)
        return result

    text = str(value).strip()
    if not text:
        return []

    # Handle accidental Postgres-array-like string forms:
    # {1482894699340501114,1482894700749918341}
    if text.startswith("{") and text.endswith("}"):
        inner = text[1:-1].strip()
        if not inner:
            return []
        result: list[str] = []
        for part in inner.split(","):
            cleaned = part.strip().strip('"').strip("'")
            if cleaned:
                result.append(cleaned)
        return result

    cleaned = text.strip('"').strip("'")
    return [cleaned] if cleaned else []


async def _get_db_configured_role_fees(conn) -> dict[str, int]:
    try:
        rows = await conn.fetch(
            """
            SELECT discord_role_id, fee_rate_bps
            FROM discord_role_fee_config
            WHERE is_active = TRUE
            """
        )
        configured: dict[str, int] = {}
        for row in rows:
            role_id = str(row["discord_role_id"] or "").strip()
            if not role_id:
                continue
            configured[role_id] = int(row["fee_rate_bps"])
        if configured:
            return configured
    except Exception:
        pass

    return dict(ROLE_FEE_RATE_BPS)


async def _get_db_configured_role_caps(conn) -> dict[str, Decimal]:
    try:
        rows = await conn.fetch(
            """
            SELECT discord_role_id, tier_key, tier_name
            FROM discord_role_fee_config
            WHERE is_active = TRUE
            """
        )
        configured: dict[str, Decimal] = {}
        for row in rows:
            role_id = str(row["discord_role_id"] or "").strip()
            if not role_id:
                continue

            tier_candidates = [
                str(row.get("tier_key") or "").strip().lower(),
                str(row.get("tier_name") or "").strip().lower(),
            ]
            cap = None
            for tier_name in tier_candidates:
                if tier_name in TIER_WAGER_CAPS:
                    cap = TIER_WAGER_CAPS[tier_name]
                    break
            if cap is not None:
                configured[role_id] = cap
        if configured:
            return configured
    except Exception:
        pass

    return dict(ROLE_WAGER_CAPS)


async def _get_wager_cap(
    conn,
    discord_uuid: str,
    *,
    role_ids: Any | None = None,
) -> Decimal | None:
    account = await _get_account_row(conn, discord_uuid)
    if not account:
        raise LookupError("casino_user_not_registered")

    live_roles = _normalize_role_ids(role_ids)
    stored_roles = _normalize_role_ids(account.get("roles"))
    roles = live_roles if live_roles else stored_roles
    role_set = set(roles)

    admin_role_ids = await _get_admin_role_ids(conn)
    if role_set & admin_role_ids:
        if live_roles:
            await _maybe_persist_live_roles(conn, discord_uuid=discord_uuid, live_role_ids=live_roles)
        return None

    configured_role_caps = await _get_db_configured_role_caps(conn)
    matched_caps = [configured_role_caps[role_id] for role_id in role_set if role_id in configured_role_caps]

    if matched_caps:
        if live_roles:
            await _maybe_persist_live_roles(conn, discord_uuid=discord_uuid, live_role_ids=live_roles)
        return max(matched_caps)

    mc_uuid = str(account.get("mc_uuid") or "").strip()
    mc_name = str(account.get("mc_name") or "").strip()
    verified_at = account.get("verified_at")
    is_registered = bool(mc_uuid or mc_name or verified_at)

    if is_registered:
        if live_roles:
            await _maybe_persist_live_roles(conn, discord_uuid=discord_uuid, live_role_ids=live_roles)
        return DEFAULT_REGISTERED_WAGER_CAP

    raise LookupError("casino_user_not_registered")


async def _get_admin_role_ids(conn) -> set[str]:
    admin_ids = set(ADMIN_ROLE_IDS)
    try:
        rows = await conn.fetch(
            """
            SELECT discord_role_id
            FROM discord_role_fee_config
            WHERE is_active = TRUE
              AND fee_rate_bps = 0
            """
        )
        for row in rows:
            role_id = str(row["discord_role_id"] or "").strip()
            if role_id:
                admin_ids.add(role_id)
    except Exception:
        pass
    return admin_ids


async def _get_account_row(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT discord_uuid, mc_uuid, mc_name, roles, verified_at
        FROM accounts
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )
    return dict(row) if row else None


async def _maybe_persist_live_roles(
    conn,
    *,
    discord_uuid: str,
    live_role_ids: list[str],
) -> None:
    if not live_role_ids:
        return
    try:
        await conn.execute(
            """
            UPDATE accounts
            SET roles = $2::text[]
            WHERE discord_uuid = $1
            """,
            discord_uuid,
            live_role_ids,
        )
    except Exception:
        pass


async def _get_fee_profile(
    conn,
    discord_uuid: str,
    requested_tier: Any | None = None,
    role_ids: Any | None = None,
) -> tuple[str, int]:
    """
    Casino fee resolution:
    1. Admin bypass role => 0%
    2. Lowest matching fee role from live Discord roles if provided, else accounts.roles
    3. If registered/linked but no fee role => default 10%
    4. If not registered/linked => reject
    """
    account = await _get_account_row(conn, discord_uuid)
    if not account:
        raise LookupError("casino_user_not_registered")

    live_roles = _normalize_role_ids(role_ids)
    stored_roles = _normalize_role_ids(account.get("roles"))
    roles = live_roles if live_roles else stored_roles
    role_set = set(roles)

    mc_uuid = str(account.get("mc_uuid") or "").strip()
    mc_name = str(account.get("mc_name") or "").strip()
    verified_at = account.get("verified_at")
    is_registered = bool(mc_uuid or mc_name or verified_at)

    configured_role_fees = await _get_db_configured_role_fees(conn)
    admin_role_ids = await _get_admin_role_ids(conn)

    if role_set & admin_role_ids:
        if live_roles:
            await _maybe_persist_live_roles(conn, discord_uuid=discord_uuid, live_role_ids=live_roles)
        return "admin", 0

    matched_role_id: Optional[str] = None
    matched_fee_bps: Optional[int] = None
    for configured_role_id, fee_bps in configured_role_fees.items():
        if configured_role_id not in role_set:
            continue
        if matched_fee_bps is None or int(fee_bps) < matched_fee_bps:
            matched_role_id = configured_role_id
            matched_fee_bps = int(fee_bps)

    if matched_role_id is not None and matched_fee_bps is not None:
        if live_roles:
            await _maybe_persist_live_roles(conn, discord_uuid=discord_uuid, live_role_ids=live_roles)
        return f"role:{matched_role_id}", matched_fee_bps

    if is_registered:
        if live_roles:
            await _maybe_persist_live_roles(conn, discord_uuid=discord_uuid, live_role_ids=live_roles)
        return "registered_default", DEFAULT_FEE_RATE_BPS

    raise LookupError("casino_user_not_registered")


def _calculate_settlement(
    *,
    wager_amount: Decimal,
    gross_payout_amount: Decimal,
    fee_rate_bps: int,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
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
    try:
        async with conn.transaction():
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
    except (asyncpg.InsufficientPrivilegeError, asyncpg.PostgresError):
        return


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


async def _execute_optional(conn, query: str, *args):
    async with conn.transaction():
        return await conn.execute(query, *args)


async def _fetchrow_optional(conn, query: str, *args):
    async with conn.transaction():
        return await conn.fetchrow(query, *args)


async def _fetchval_optional(conn, query: str, *args):
    async with conn.transaction():
        return await conn.fetchval(query, *args)


async def _insert_balance_transaction_compat(
    conn,
    *,
    discord_uuid: str,
    preferred_kind: str,
    amount: Decimal,
    metadata: Dict[str, Any] | None = None,
) -> str:
    try:
        async with conn.transaction():
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
        async with conn.transaction():
            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=discord_uuid,
                kind="admin_add",
                amount=amount,
                metadata=fallback_metadata,
            )
        return "admin_add"


async def get_user(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    await ensure_schema(conn)
    row = await _fetchrow_optional(
        conn,
        """
        SELECT discord_uuid, sender_external_id, balance, created_at, updated_at
        FROM casino_users
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )
    return _row_to_dict(row)


async def upsert_user(conn, discord_uuid: str, sender_external_id: str) -> Dict[str, Any]:
    await ensure_schema(conn)
    row = await _fetchrow_optional(
        conn,
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


async def upsert_pf_params(conn, discord_uuid: str, client_seed: str, server_seed: str, nonce: int = 0) -> Dict[str, Any]:
    await ensure_schema(conn)
    try:
        row = await _fetchrow_optional(
            conn,
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
    except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError) as exc:
        # In restricted environments, casino schema may not exist or be writable.
        # Avoid raising 500 for PF storage unavailability; let caller continue with fallback.
        logger.warning(
            "casino_pf_upsert_storage_unavailable discord_uuid=%s error=%s",
            discord_uuid,
            exc.__class__.__name__,
        )
        return {
            "discord_uuid": discord_uuid,
            "client_seed": client_seed,
            "server_seed": server_seed,
            "nonce": int(nonce),
            "created_at": None,
            "updated_at": None,
            "persisted": False,
        }


async def save_pf_params(
    conn,
    discord_uuid: str,
    client_seed: str,
    server_seed: str,
    nonce: int = 0,
) -> Dict[str, Any]:
    return await upsert_pf_params(conn, discord_uuid, client_seed, server_seed, nonce)


async def get_pf_params(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    await ensure_schema(conn)
    try:
        row = await _fetchrow_optional(
            conn,
            """
            SELECT discord_uuid, client_seed, server_seed, nonce, created_at, updated_at
            FROM casino_pf_params
            WHERE discord_uuid = $1
            """,
            discord_uuid,
        )
        return _row_to_dict(row)
    except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError) as exc:
        logger.warning(
            "casino_pf_get_storage_unavailable discord_uuid=%s error=%s",
            discord_uuid,
            exc.__class__.__name__,
        )
        return None


async def increment_pf_nonce(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    await ensure_schema(conn)
    try:
        row = await _fetchrow_optional(
            conn,
            """
            UPDATE casino_pf_params
            SET nonce = nonce + 1,
                updated_at = NOW()
            WHERE discord_uuid = $1
            RETURNING discord_uuid, client_seed, server_seed, nonce, created_at, updated_at
            """,
            discord_uuid,
        )
        return _row_to_dict(row)
    except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError):
        return None


async def set_account_panel_message(conn, message_id: int) -> Dict[str, Any]:
    await ensure_schema(conn)
    row = await _fetchrow_optional(
        conn,
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
    row = await _fetchrow_optional(
        conn,
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
    row = await _fetchrow_optional(
        conn,
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
    async with conn.transaction():
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
    status = await _execute_optional(
        conn,
        """
        DELETE FROM casino_tables
        WHERE channel_id = $1
        """,
        channel_id,
    )
    return _extract_affected_count(status) > 0


async def clear_tables(conn) -> int:
    await ensure_schema(conn)
    status = await _execute_optional(conn, "DELETE FROM casino_tables")
    return _extract_affected_count(status)


async def count_tables(conn) -> int:
    await ensure_schema(conn)
    count = await _fetchval_optional(conn, "SELECT COUNT(*) FROM casino_tables")
    return int(count or 0)


async def start_game_session(
    conn,
    *,
    discord_uuid: str,
    game_type: str,
    wager_amount: Any,
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    wager_amount = _to_decimal(wager_amount)
    if wager_amount <= Decimal("0"):
        raise ValueError("invalid_wager_amount")

    clean_metadata = _coerce_metadata_dict(metadata)
    live_role_ids = _normalize_role_ids(clean_metadata.get("discord_role_ids"))

    account = await _get_account_row(conn, discord_uuid)
    if account is None:
        raise LookupError("casino_user_not_registered")

    await _get_fee_profile(conn, discord_uuid, role_ids=live_role_ids)

    wager_cap = await _get_wager_cap(conn, discord_uuid, role_ids=live_role_ids)
    if wager_cap is not None and wager_amount > wager_cap:
        raise ValueError(f"wager_cap_exceeded:{int(wager_cap)}")

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
        INSERT INTO casino_game_sessions(discord_uuid, game_type, wager_amount, status, metadata)
        VALUES ($1, $2, $3, 'open', $4::jsonb)
        RETURNING *
        """,
        discord_uuid,
        str(game_type).strip().lower(),
        wager_amount,
        json.dumps(clean_metadata),
    )

    balance_tx_metadata = {
        "game_type": str(game_type).strip().lower(),
        "session_id": session["id"],
        **clean_metadata,
    }
    applied_balance_kind = await _insert_balance_transaction_compat(
        conn,
        discord_uuid=discord_uuid,
        preferred_kind="gambling_wager",
        amount=-wager_amount,
        metadata=balance_tx_metadata,
    )

    try:
        await _execute_optional(
            conn,
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
    return {
        "session": payload,
        "balance": new_balance,
        "current_balance": new_balance,
        "applied_balance_transaction_kind": applied_balance_kind,
    }


async def settle_game_session(
    conn,
    *,
    session_id: int,
    gross_payout_amount: Any,
    outcome: str,
    metadata: Dict[str, Any] | None = None,
    requested_tier: Any | None = None,
) -> Dict[str, Any]:
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

    session_status = str(session_row["status"] or "").strip().lower()
    if session_status not in {"open", "started"}:
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

    existing_metadata = _coerce_metadata_dict(session_row["metadata"])
    incoming_metadata = _coerce_metadata_dict(metadata)

    live_role_ids = _normalize_role_ids(
        incoming_metadata.get("discord_role_ids") or existing_metadata.get("discord_role_ids")
    )

    fee_profile_key, fee_rate_bps = await _get_fee_profile(
        conn,
        discord_uuid,
        requested_tier=requested_tier,
        role_ids=live_role_ids,
    )

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

    merged_metadata = dict(existing_metadata)
    merged_metadata.update(incoming_metadata)
    merged_metadata.update(
        {
            "session_id": session_id,
            "game_type": session_row["game_type"],
            "outcome": outcome,
            "gross_payout_amount": str(gross_payout_amount),
            "fee_amount": str(fee_amount),
            "net_payout_amount": str(net_payout_amount),
            "fee_profile_key": fee_profile_key,
            "fee_rate_bps": fee_rate_bps,
        }
    )
    if live_role_ids:
        merged_metadata["discord_role_ids"] = live_role_ids

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
        fee_profile_key,
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
        await _execute_optional(
            conn,
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
    return {
        "session": payload,
        "balance": new_balance,
        "current_balance": new_balance,
        "applied_balance_transaction_kind": applied_balance_kind,
    }
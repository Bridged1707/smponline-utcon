from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


REGISTER_CHALLENGE_ITEMS = [
    {"item_type": "DIRT", "item_name": "Dirt"},
    {"item_type": "WHEAT_SEEDS", "item_name": "Wheat Seeds"},
    {"item_type": "PAPER", "item_name": "Paper"},
    {"item_type": "COBBLESTONE", "item_name": "Cobblestone"},
]
REGISTER_MIN_VALUE = 1
REGISTER_MAX_VALUE = 999
REGISTER_DEFAULT_TTL_MINUTES = 360
REGISTER_DEFAULT_SHOP_TYPE = "SELLING"
REGISTER_STATUS_PENDING = "pending"
REGISTER_STATUS_MATCHED = "matched"
REGISTER_STATUS_EXPIRED = "expired"
REGISTER_STATUS_FAILED = "failed"
REGISTER_STATUS_CANCELLED = "cancelled"


async def ensure_account_schema(conn) -> None:
    await conn.execute(
        """
        ALTER TABLE accounts
        ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_register_queue (
            id BIGSERIAL PRIMARY KEY,
            discord_uuid TEXT NOT NULL,
            challenge_item_type TEXT NOT NULL,
            challenge_item_name TEXT,
            challenge_price NUMERIC NOT NULL,
            challenge_item_quantity INTEGER NOT NULL,
            challenge_shop_type TEXT NOT NULL DEFAULT 'SELLING',
            status TEXT NOT NULL DEFAULT 'pending',
            requested_at TIMESTAMP NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMP NOT NULL,
            matched_shop_id BIGINT,
            matched_owner_uuid UUID,
            matched_owner_name TEXT,
            resolved_at TIMESTAMP,
            failure_reason TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_account_register_queue_status
        ON account_register_queue(status)
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_account_register_queue_discord_uuid
        ON account_register_queue(discord_uuid)
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_account_register_queue_match
        ON account_register_queue(status, challenge_item_type, challenge_price, challenge_item_quantity)
        """
    )


async def create_account(conn, discord_uuid: str, mc_uuid: str, mc_name: str):
    await conn.execute(
        """
        INSERT INTO accounts(discord_uuid, mc_uuid, mc_name, verified_at)
        VALUES($1,$2,$3,NOW())
        ON CONFLICT (discord_uuid)
        DO UPDATE SET
            mc_uuid = EXCLUDED.mc_uuid,
            mc_name = EXCLUDED.mc_name,
            verified_at = EXCLUDED.verified_at
        """,
        discord_uuid,
        mc_uuid,
        mc_name,
    )

    await conn.execute(
        """
        INSERT INTO balances(discord_uuid, balance)
        VALUES($1,0)
        ON CONFLICT (discord_uuid) DO NOTHING
        """,
        discord_uuid,
    )


async def get_account_by_discord_uuid(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT id, discord_uuid, mc_uuid, mc_name, created_at, verified_at, roles, rates
        FROM accounts
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )
    return dict(row) if row else None


async def get_account_by_mc_uuid(conn, mc_uuid: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT id, discord_uuid, mc_uuid, mc_name, created_at, verified_at, roles, rates
        FROM accounts
        WHERE mc_uuid = $1
        """,
        mc_uuid,
    )
    return dict(row) if row else None


async def get_pending_registration_for_discord(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT *
        FROM account_register_queue
        WHERE discord_uuid = $1
          AND status = $2
        ORDER BY requested_at DESC, id DESC
        LIMIT 1
        """,
        discord_uuid,
        REGISTER_STATUS_PENDING,
    )
    return dict(row) if row else None


async def get_registration_queue_item(conn, queue_id: int) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        "SELECT * FROM account_register_queue WHERE id = $1",
        queue_id,
    )
    return dict(row) if row else None


async def list_pending_registration_queue(conn, limit: int = 100) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM account_register_queue
        WHERE status = $1
        ORDER BY requested_at ASC, id ASC
        LIMIT $2
        """,
        REGISTER_STATUS_PENDING,
        limit,
    )
    return [dict(row) for row in rows]


async def expire_stale_registrations(conn) -> int:
    result = await conn.execute(
        """
        UPDATE account_register_queue
        SET status = $1,
            resolved_at = NOW(),
            failure_reason = COALESCE(failure_reason, 'challenge expired')
        WHERE status = $2
          AND expires_at <= NOW()
        """,
        REGISTER_STATUS_EXPIRED,
        REGISTER_STATUS_PENDING,
    )
    return _extract_affected_count(result)


async def create_registration_challenge(
    conn,
    discord_uuid: str,
    ttl_minutes: int = REGISTER_DEFAULT_TTL_MINUTES,
) -> Dict[str, Any]:
    existing = await get_pending_registration_for_discord(conn, discord_uuid)
    if existing is not None:
        return existing

    account = await get_account_by_discord_uuid(conn, discord_uuid)
    if account and account.get("mc_uuid"):
        raise ValueError("discord account is already registered")

    challenge = _generate_registration_challenge()
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)

    row = await conn.fetchrow(
        """
        INSERT INTO account_register_queue(
            discord_uuid,
            challenge_item_type,
            challenge_item_name,
            challenge_price,
            challenge_item_quantity,
            challenge_shop_type,
            status,
            expires_at
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)
        RETURNING *
        """,
        discord_uuid,
        challenge["item_type"],
        challenge["item_name"],
        challenge["price"],
        challenge["item_quantity"],
        challenge["shop_type"],
        REGISTER_STATUS_PENDING,
        expires_at.replace(tzinfo=None),
    )
    return dict(row)


async def resolve_registration_match(
    conn,
    queue_id: int,
    matched_shop_id: int,
    matched_owner_uuid: str,
    matched_owner_name: str,
) -> Dict[str, Any]:
    queue_item = await get_registration_queue_item(conn, queue_id)
    if queue_item is None:
        raise LookupError("registration queue item not found")

    if queue_item["status"] != REGISTER_STATUS_PENDING:
        raise ValueError(f"registration queue item is not pending: {queue_item['status']}")

    if queue_item["expires_at"] <= datetime.utcnow():
        await mark_registration_status(
            conn,
            queue_id=queue_id,
            status=REGISTER_STATUS_EXPIRED,
            failure_reason="challenge expired before resolution",
        )
        raise ValueError("registration queue item has expired")

    existing_mc_account = await get_account_by_mc_uuid(conn, matched_owner_uuid)
    if existing_mc_account and existing_mc_account["discord_uuid"] != queue_item["discord_uuid"]:
        await mark_registration_status(
            conn,
            queue_id=queue_id,
            status=REGISTER_STATUS_FAILED,
            failure_reason="minecraft account is already linked to another discord account",
        )
        raise ValueError("minecraft account is already linked to another discord account")

    await create_account(
        conn,
        discord_uuid=queue_item["discord_uuid"],
        mc_uuid=matched_owner_uuid,
        mc_name=matched_owner_name,
    )

    row = await conn.fetchrow(
        """
        UPDATE account_register_queue
        SET status = $1,
            matched_shop_id = $2,
            matched_owner_uuid = $3::uuid,
            matched_owner_name = $4,
            resolved_at = NOW(),
            failure_reason = NULL,
            attempt_count = attempt_count + 1
        WHERE id = $5
        RETURNING *
        """,
        REGISTER_STATUS_MATCHED,
        matched_shop_id,
        matched_owner_uuid,
        matched_owner_name,
        queue_id,
    )
    return dict(row)


async def mark_registration_status(
    conn,
    queue_id: int,
    status: str,
    failure_reason: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE account_register_queue
        SET status = $1,
            resolved_at = NOW(),
            failure_reason = $2,
            attempt_count = attempt_count + 1
        WHERE id = $3
        RETURNING *
        """,
        status,
        failure_reason,
        queue_id,
    )
    return dict(row) if row else None


async def find_registration_match_candidates(
    conn,
    item_type: str,
    price: float,
    item_quantity: int,
    shop_type: str,
    last_seen_since_ts: Optional[int] = None,
) -> List[Dict[str, Any]]:
    params = [item_type, price, item_quantity, shop_type]
    where = [
        "item_type = $1",
        "price = $2",
        "item_quantity = $3",
        "shop_type = $4",
        "remaining > 0",
    ]
    if last_seen_since_ts is not None:
        params.append(last_seen_since_ts)
        where.append(f"last_seen >= ${len(params)}")

    rows = await conn.fetch(
        f"""
        SELECT
            shop_id,
            owner_name,
            owner_uuid,
            world,
            x,
            y,
            z,
            shop_type,
            price,
            remaining,
            item_type,
            item_name,
            item_quantity,
            snbt,
            last_seen
        FROM shops
        WHERE {' AND '.join(where)}
        ORDER BY last_seen DESC, shop_id DESC
        """,
        *params,
    )
    return [dict(row) for row in rows]


def _extract_affected_count(result: str) -> int:
    try:
        return int(str(result).split()[-1])
    except Exception:
        return 0


def _generate_registration_challenge() -> Dict[str, Any]:
    item = random.choice(REGISTER_CHALLENGE_ITEMS)
    return {
        "item_type": item["item_type"],
        "item_name": item["item_name"],
        "price": random.randint(REGISTER_MIN_VALUE, REGISTER_MAX_VALUE),
        "item_quantity": random.randint(REGISTER_MIN_VALUE, REGISTER_MAX_VALUE),
        "shop_type": REGISTER_DEFAULT_SHOP_TYPE,
    }


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


async def create_account(conn, discord_uuid: str, mc_uuid: str, mc_name: str | None = None):
    await ensure_account_schema(conn)

    resolved_mc_name = mc_name or mc_uuid

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
        resolved_mc_name,
    )

    await conn.execute(
        """
        INSERT INTO balances(discord_uuid, balance)
        VALUES($1,0)
        ON CONFLICT (discord_uuid) DO NOTHING
        """,
        discord_uuid,
    )


async def delete_account(conn, discord_uuid: str) -> bool:
    await ensure_account_schema(conn)

    account = await conn.fetchrow(
        """
        SELECT discord_uuid
        FROM accounts
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )

    if not account:
        return False

    await conn.execute(
        """
        DELETE FROM balances
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )

    await conn.execute(
        """
        DELETE FROM accounts
        WHERE discord_uuid = $1
        """,
        discord_uuid,
    )

    return True


async def get_account_by_discord_uuid(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    await ensure_account_schema(conn)

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
    await ensure_account_schema(conn)

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
    await ensure_account_schema(conn)

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
    await ensure_account_schema(conn)

    row = await conn.fetchrow(
        "SELECT * FROM account_register_queue WHERE id = $1",
        queue_id,
    )
    return dict(row) if row else None


async def list_pending_registration_queue(conn, limit: int = 100) -> List[Dict[str, Any]]:
    await ensure_account_schema(conn)

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
    await ensure_account_schema(conn)

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
    await ensure_account_schema(conn)

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
        challenge["quantity"],
        REGISTER_DEFAULT_SHOP_TYPE,
        REGISTER_STATUS_PENDING,
        expires_at,
    )
    return dict(row)


async def mark_registration_matched(
    conn,
    queue_id: int,
    *,
    matched_shop_id: int | None,
    matched_owner_uuid: str | None,
    matched_owner_name: str | None,
    mc_uuid: str,
    mc_name: str | None = None,
) -> Dict[str, Any]:
    await ensure_account_schema(conn)

    queue_item = await get_registration_queue_item(conn, queue_id)
    if queue_item is None:
        raise LookupError("registration queue item not found")

    discord_uuid = queue_item["discord_uuid"]

    await create_account(conn, discord_uuid=discord_uuid, mc_uuid=mc_uuid, mc_name=mc_name)

    row = await conn.fetchrow(
        """
        UPDATE account_register_queue
        SET status = $2,
            matched_shop_id = $3,
            matched_owner_uuid = $4,
            matched_owner_name = $5,
            resolved_at = NOW(),
            failure_reason = NULL
        WHERE id = $1
        RETURNING *
        """,
        queue_id,
        REGISTER_STATUS_MATCHED,
        matched_shop_id,
        matched_owner_uuid,
        matched_owner_name,
    )
    return dict(row)


async def mark_registration_failed(conn, queue_id: int, reason: str) -> Dict[str, Any]:
    await ensure_account_schema(conn)

    row = await conn.fetchrow(
        """
        UPDATE account_register_queue
        SET status = $2,
            resolved_at = NOW(),
            failure_reason = $3
        WHERE id = $1
        RETURNING *
        """,
        queue_id,
        REGISTER_STATUS_FAILED,
        reason,
    )
    if row is None:
        raise LookupError("registration queue item not found")
    return dict(row)


async def increment_registration_attempt_count(conn, queue_id: int) -> Dict[str, Any]:
    await ensure_account_schema(conn)

    row = await conn.fetchrow(
        """
        UPDATE account_register_queue
        SET attempt_count = attempt_count + 1
        WHERE id = $1
        RETURNING *
        """,
        queue_id,
    )
    if row is None:
        raise LookupError("registration queue item not found")
    return dict(row)


async def get_registration_status(conn, discord_uuid: str) -> Dict[str, Any]:
    await ensure_account_schema(conn)
    await expire_stale_registrations(conn)

    account = await get_account_by_discord_uuid(conn, discord_uuid)
    if account and account.get("mc_uuid"):
        return {
            "status": "already_registered",
            "discord_uuid": discord_uuid,
            "mc_uuid": account.get("mc_uuid"),
            "mc_name": account.get("mc_name"),
            "verified_at": account.get("verified_at"),
        }

    pending = await get_pending_registration_for_discord(conn, discord_uuid)
    if pending is not None:
        return {
            "status": REGISTER_STATUS_PENDING,
            "discord_uuid": discord_uuid,
            "challenge": {
                "id": pending["id"],
                "item_type": pending["challenge_item_type"],
                "item_name": pending["challenge_item_name"],
                "price": pending["challenge_price"],
                "item_quantity": pending["challenge_item_quantity"],
                "shop_type": pending["challenge_shop_type"],
                "requested_at": pending["requested_at"],
                "expires_at": pending["expires_at"],
                "attempt_count": pending["attempt_count"],
            },
        }

    row = await conn.fetchrow(
        """
        SELECT *
        FROM account_register_queue
        WHERE discord_uuid = $1
        ORDER BY requested_at DESC, id DESC
        LIMIT 1
        """,
        discord_uuid,
    )
    if row is None:
        return {"status": "not_found", "discord_uuid": discord_uuid}

    latest = dict(row)
    return {
        "status": latest["status"],
        "discord_uuid": discord_uuid,
        "failure_reason": latest.get("failure_reason"),
        "challenge": {
            "id": latest["id"],
            "item_type": latest["challenge_item_type"],
            "item_name": latest["challenge_item_name"],
            "price": latest["challenge_price"],
            "item_quantity": latest["challenge_item_quantity"],
            "shop_type": latest["challenge_shop_type"],
            "requested_at": latest["requested_at"],
            "expires_at": latest["expires_at"],
            "attempt_count": latest["attempt_count"],
            "matched_shop_id": latest.get("matched_shop_id"),
            "matched_owner_uuid": latest.get("matched_owner_uuid"),
            "matched_owner_name": latest.get("matched_owner_name"),
            "resolved_at": latest.get("resolved_at"),
        },
    }


def _generate_registration_challenge() -> Dict[str, Any]:
    item = random.choice(REGISTER_CHALLENGE_ITEMS)
    return {
        "item_type": item["item_type"],
        "item_name": item["item_name"],
        "price": random.randint(REGISTER_MIN_VALUE, REGISTER_MAX_VALUE),
        "quantity": 1,
    }


def _extract_affected_count(result: str) -> int:
    try:
        return int(result.rsplit(" ", 1)[-1])
    except Exception:
        return 0
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

PAID_TIERS = {"pro", "garry"}
ALL_TIERS = {"free", "pro", "garry"}


async def ensure_membership_schema(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS memberships (
            id BIGSERIAL PRIMARY KEY,
            discord_uuid TEXT NOT NULL UNIQUE,
            tier TEXT NOT NULL,
            starts_at TIMESTAMP NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMP,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            CONSTRAINT memberships_tier_chk CHECK (tier IN ('free', 'pro', 'garry')),
            CONSTRAINT memberships_expiry_chk CHECK (
                (tier = 'free' AND expires_at IS NULL)
                OR
                (tier IN ('pro', 'garry') AND expires_at IS NOT NULL)
            )
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS membership_history (
            id BIGSERIAL PRIMARY KEY,
            discord_uuid TEXT NOT NULL,
            old_tier TEXT,
            new_tier TEXT,
            changed_at TIMESTAMP NOT NULL DEFAULT NOW(),
            reason TEXT
        )
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_membership_history_discord_uuid
        ON membership_history(discord_uuid, changed_at DESC)
        """
    )


async def get_membership_row(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    await ensure_membership_schema(conn)
    await expire_memberships(conn)
    row = await conn.fetchrow(
        """
        SELECT id, discord_uuid, tier, starts_at, expires_at, is_active, created_at, updated_at
        FROM memberships
        WHERE discord_uuid = $1
        LIMIT 1
        """,
        discord_uuid,
    )
    return dict(row) if row else None


async def expire_memberships(conn) -> int:
    await ensure_membership_schema(conn)
    result = await conn.execute(
        """
        UPDATE memberships
        SET is_active = FALSE,
            updated_at = NOW()
        WHERE is_active = TRUE
          AND expires_at IS NOT NULL
          AND expires_at <= NOW()
        """
    )
    return _extract_affected_count(result)


async def get_effective_membership(conn, discord_uuid: str) -> Dict[str, Any]:
    from utcon.repositories import account as account_repo

    await ensure_membership_schema(conn)
    await expire_memberships(conn)

    account = await account_repo.get_account_by_discord_uuid(conn, discord_uuid)
    membership = await get_membership_row(conn, discord_uuid)

    if not account:
        return {
            "discord_uuid": discord_uuid,
            "membership_type": "unregistered",
            "tier": None,
            "starts_at": None,
            "expires_at": None,
            "is_active": False,
            "is_registered": False,
            "source": "none",
        }

    if membership and membership.get("is_active") and membership.get("tier") in ALL_TIERS:
        tier = membership["tier"]
        return {
            "discord_uuid": discord_uuid,
            "membership_type": tier,
            "tier": tier,
            "starts_at": membership.get("starts_at"),
            "expires_at": membership.get("expires_at"),
            "is_active": True,
            "is_registered": True,
            "source": "memberships",
        }

    return {
        "discord_uuid": discord_uuid,
        "membership_type": "free",
        "tier": "free",
        "starts_at": account.get("verified_at") or account.get("created_at"),
        "expires_at": None,
        "is_active": True,
        "is_registered": True,
        "source": "accounts",
    }


async def upsert_membership(
    conn,
    *,
    discord_uuid: str,
    tier: str,
    duration_days: int = 7,
    reason: str | None = None,
    replace_active: bool = True,
) -> Dict[str, Any]:
    from utcon.repositories import account as account_repo

    await ensure_membership_schema(conn)

    if tier not in ALL_TIERS:
        raise ValueError("invalid membership tier")

    account = await account_repo.get_account_by_discord_uuid(conn, discord_uuid)
    if not account:
        raise LookupError("account not found")

    current = await get_membership_row(conn, discord_uuid)
    now = datetime.utcnow()

    if tier == "free":
        row = await conn.fetchrow(
            """
            INSERT INTO memberships(discord_uuid, tier, starts_at, expires_at, is_active, updated_at)
            VALUES($1, 'free', NOW(), NULL, TRUE, NOW())
            ON CONFLICT (discord_uuid)
            DO UPDATE SET
                tier = EXCLUDED.tier,
                starts_at = NOW(),
                expires_at = NULL,
                is_active = TRUE,
                updated_at = NOW()
            RETURNING id, discord_uuid, tier, starts_at, expires_at, is_active, created_at, updated_at
            """,
            discord_uuid,
        )
        await _insert_history(conn, discord_uuid, current.get("tier") if current else None, "free", reason)
        return dict(row)

    if current and current.get("tier") == tier and current.get("is_active") and not replace_active:
        base_expiry = current.get("expires_at") or now
        new_expires_at = max(base_expiry, now) + timedelta(days=duration_days)
        row = await conn.fetchrow(
            """
            UPDATE memberships
            SET expires_at = $2,
                is_active = TRUE,
                updated_at = NOW()
            WHERE discord_uuid = $1
            RETURNING id, discord_uuid, tier, starts_at, expires_at, is_active, created_at, updated_at
            """,
            discord_uuid,
            new_expires_at,
        )
        await _insert_history(conn, discord_uuid, tier, tier, reason or f"extended by {duration_days} days")
        return dict(row)

    new_expires_at = now + timedelta(days=duration_days)
    row = await conn.fetchrow(
        """
        INSERT INTO memberships(discord_uuid, tier, starts_at, expires_at, is_active, updated_at)
        VALUES($1, $2, NOW(), $3, TRUE, NOW())
        ON CONFLICT (discord_uuid)
        DO UPDATE SET
            tier = EXCLUDED.tier,
            starts_at = NOW(),
            expires_at = EXCLUDED.expires_at,
            is_active = TRUE,
            updated_at = NOW()
        RETURNING id, discord_uuid, tier, starts_at, expires_at, is_active, created_at, updated_at
        """,
        discord_uuid,
        tier,
        new_expires_at,
    )
    await _insert_history(conn, discord_uuid, current.get("tier") if current else None, tier, reason)
    return dict(row)


async def _insert_history(conn, discord_uuid: str, old_tier: str | None, new_tier: str | None, reason: str | None) -> None:
    await conn.execute(
        """
        INSERT INTO membership_history(discord_uuid, old_tier, new_tier, reason)
        VALUES($1, $2, $3, $4)
        """,
        discord_uuid,
        old_tier,
        new_tier,
        reason,
    )


def _extract_affected_count(result: str) -> int:
    try:
        return int(result.rsplit(" ", 1)[-1])
    except Exception:
        return 0
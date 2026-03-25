from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional

ALL_TIERS = {"free", "pro", "garry"}
PAID_TIERS = {"pro", "garry"}
MEMBERSHIP_PRICES = {
    "pro": 7,
    "garry": 14,
}


async def get_membership_row(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
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


async def purchase_membership(
    conn,
    *,
    discord_uuid: str,
    tier: str,
    weeks: int | None,
    amount: int | None,
) -> Dict[str, Any]:
    from utcon.repositories import account as account_repo

    if tier not in PAID_TIERS:
        raise ValueError("only paid memberships can be purchased")

    account = await account_repo.get_account_by_discord_uuid(conn, discord_uuid)
    if not account:
        raise LookupError("account not found")

    price_per_week = MEMBERSHIP_PRICES[tier]

    if weeks is None and amount is None:
        raise ValueError("either weeks or amount must be provided")
    if weeks is not None and amount is not None:
        raise ValueError("provide either weeks or amount, not both")

    if amount is not None:
        if amount % price_per_week != 0:
            raise ValueError(f"{tier} membership costs {price_per_week} diamonds per week")
        purchase_weeks = amount // price_per_week
        total_cost = amount
    else:
        purchase_weeks = weeks or 0
        total_cost = purchase_weeks * price_per_week

    if purchase_weeks <= 0:
        raise ValueError("purchase weeks must be greater than zero")

    await expire_memberships(conn)
    current = await get_membership_row(conn, discord_uuid)

    if current and current.get("is_active") and current.get("tier") in PAID_TIERS and current.get("tier") != tier:
        raise ValueError(
            f"you already have an active {current['tier']} membership; cross-tier paid switching is blocked"
        )

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
        raise LookupError("balance not found")

    current_balance = Decimal(balance_row["balance"])
    total_cost_decimal = Decimal(total_cost)

    if current_balance < total_cost_decimal:
        raise ValueError(
            f"insufficient balance: need {total_cost} diamonds, have {int(current_balance)}"
        )

    new_balance_row = await conn.fetchrow(
        """
        UPDATE balances
        SET balance = balance - $2,
            last_updated = NOW()
        WHERE discord_uuid = $1
        RETURNING balance, last_updated
        """,
        discord_uuid,
        total_cost_decimal,
    )

    membership_row = await upsert_membership(
        conn,
        discord_uuid=discord_uuid,
        tier=tier,
        duration_days=purchase_weeks * 7,
        reason=f"membership purchase: {tier} x {purchase_weeks} week(s) for {total_cost} diamonds",
        replace_active=False,
    )

    await conn.execute(
        """
        INSERT INTO balance_transactions(discord_uuid, kind, amount, metadata)
        VALUES($1, 'membership_purchase', $2, $3::jsonb)
        """,
        discord_uuid,
        total_cost_decimal,
        (
            "{"
            f"\"tier\": \"{tier}\", "
            f"\"weeks\": {purchase_weeks}, "
            f"\"diamonds_spent\": {total_cost}"
            "}"
        ),
    )

    await _insert_history(
        conn,
        discord_uuid,
        current.get("tier") if current else None,
        tier,
        f"purchase: {purchase_weeks} week(s), {total_cost} diamonds",
    )

    return {
        "discord_uuid": discord_uuid,
        "tier": tier,
        "weeks": purchase_weeks,
        "diamonds_spent": total_cost,
        "price_per_week": price_per_week,
        "membership": {
            "id": membership_row["id"],
            "tier": membership_row["tier"],
            "starts_at": membership_row["starts_at"],
            "expires_at": membership_row["expires_at"],
            "is_active": membership_row["is_active"],
            "updated_at": membership_row["updated_at"],
        },
        "balance": {
            "before": int(current_balance),
            "after": int(Decimal(new_balance_row["balance"])),
            "last_updated": new_balance_row["last_updated"],
        },
    }


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
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from utcon.repositories import account as account_repo
from utcon.repositories import membership as membership_repo

WEEKLY_RESET_TIMEZONE = ZoneInfo("America/New_York")
ALLOWED_TIERS = {"free", "pro", "garry"}


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_command_name(command: str) -> str:
    cleaned = (command or "").strip().lower()
    if not cleaned:
        raise ValueError("command is required")
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned


def _compute_credit_window(now_utc_naive: datetime) -> tuple[datetime, datetime]:
    now_utc_aware = now_utc_naive.replace(tzinfo=timezone.utc)
    local_now = now_utc_aware.astimezone(WEEKLY_RESET_TIMEZONE)

    week_start_local = (
        local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=local_now.weekday())
    )
    week_end_local = week_start_local + timedelta(days=7)

    week_start_utc = week_start_local.astimezone(timezone.utc).replace(tzinfo=None)
    week_end_utc = week_end_local.astimezone(timezone.utc).replace(tzinfo=None)
    return week_start_utc, week_end_utc


async def get_credit_tier_config(conn, tier: str) -> dict:
    row = await conn.fetchrow(
        """
        SELECT tier, weekly_credits
        FROM credit_tier_config
        WHERE tier = $1
        """,
        tier,
    )
    if row is None:
        raise LookupError("credit_tier_config_not_found")
    return dict(row)


async def get_command_credit_cost(conn, command: str, tier: str) -> dict:
    row = await conn.fetchrow(
        """
        SELECT command, tier, cost_credits, is_active, updated_at
        FROM command_credit_costs
        WHERE command = $1
          AND tier = $2
        """,
        command,
        tier,
    )
    if row is None:
        raise LookupError("command_credit_cost_not_found")
    return dict(row)


async def get_wallet_for_update(conn, discord_uuid: str) -> dict | None:
    row = await conn.fetchrow(
        """
        SELECT discord_uuid, week_start_at, week_end_at, weekly_credits, used_credits, updated_at
        FROM user_credit_wallets
        WHERE discord_uuid = $1
        FOR UPDATE
        """,
        discord_uuid,
    )
    return dict(row) if row else None


async def upsert_wallet_window(
    conn,
    *,
    discord_uuid: str,
    weekly_credits: int,
    week_start_at: datetime,
    week_end_at: datetime,
) -> dict:
    row = await conn.fetchrow(
        """
        INSERT INTO user_credit_wallets(
            discord_uuid,
            week_start_at,
            week_end_at,
            weekly_credits,
            used_credits,
            updated_at
        )
        VALUES($1, $2, $3, $4, 0, NOW())
        ON CONFLICT (discord_uuid)
        DO UPDATE SET
            week_start_at = EXCLUDED.week_start_at,
            week_end_at = EXCLUDED.week_end_at,
            weekly_credits = EXCLUDED.weekly_credits,
            used_credits = 0,
            updated_at = NOW()
        RETURNING discord_uuid, week_start_at, week_end_at, weekly_credits, used_credits, updated_at
        """,
        discord_uuid,
        week_start_at,
        week_end_at,
        weekly_credits,
    )
    return dict(row)


async def get_active_wallet(
    conn,
    *,
    discord_uuid: str,
    weekly_credits: int,
    now_utc_naive: datetime,
) -> dict:
    wallet = await get_wallet_for_update(conn, discord_uuid)
    current_week_start, current_week_end = _compute_credit_window(now_utc_naive)

    if wallet is None:
        return await upsert_wallet_window(
            conn,
            discord_uuid=discord_uuid,
            weekly_credits=weekly_credits,
            week_start_at=current_week_start,
            week_end_at=current_week_end,
        )

    wallet_week_end = wallet["week_end_at"]
    if wallet_week_end <= now_utc_naive:
        return await upsert_wallet_window(
            conn,
            discord_uuid=discord_uuid,
            weekly_credits=weekly_credits,
            week_start_at=current_week_start,
            week_end_at=current_week_end,
        )

    if wallet["weekly_credits"] != weekly_credits:
        row = await conn.fetchrow(
            """
            UPDATE user_credit_wallets
            SET weekly_credits = $2,
                updated_at = NOW()
            WHERE discord_uuid = $1
            RETURNING discord_uuid, week_start_at, week_end_at, weekly_credits, used_credits, updated_at
            """,
            discord_uuid,
            weekly_credits,
        )
        return dict(row)

    return wallet


async def insert_credit_usage_ledger(
    conn,
    *,
    discord_uuid: str,
    command: str,
    tier: str,
    charged_credits: int,
    week_start_at: datetime,
    metadata: dict | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO credit_usage_ledger(
            discord_uuid,
            command,
            tier,
            charged_credits,
            week_start_at,
            metadata
        )
        VALUES($1, $2, $3, $4, $5, $6::jsonb)
        """,
        discord_uuid,
        command,
        tier,
        charged_credits,
        week_start_at,
        json.dumps(metadata or {}),
    )


async def consume_command_credits(
    conn,
    *,
    discord_uuid: str,
    command: str,
    dry_run: bool = False,
    metadata: dict | None = None,
) -> dict:
    now_utc_naive = _utc_now_naive()
    normalized_command = _normalize_command_name(command)

    account = await account_repo.get_account_by_discord_uuid(conn, discord_uuid)
    if not account:
        raise LookupError("account_not_registered")

    membership = await membership_repo.get_effective_membership(conn, discord_uuid)
    tier = (membership.get("tier") or "").lower()

    if tier not in ALLOWED_TIERS:
        raise LookupError("membership_tier_not_supported")

    tier_config = await get_credit_tier_config(conn, tier)
    command_cost = await get_command_credit_cost(conn, normalized_command, tier)

    if not command_cost.get("is_active", True):
        raise LookupError("command_credit_cost_inactive")

    weekly_credits = int(tier_config["weekly_credits"])
    charged_credits = int(command_cost["cost_credits"])

    wallet = await get_active_wallet(
        conn,
        discord_uuid=discord_uuid,
        weekly_credits=weekly_credits,
        now_utc_naive=now_utc_naive,
    )

    used_credits_before = int(wallet["used_credits"])
    remaining_before = max(weekly_credits - used_credits_before, 0)

    if charged_credits > remaining_before:
        return {
            "allowed": False,
            "reason": "insufficient_command_credits",
            "discord_uuid": discord_uuid,
            "command": normalized_command,
            "tier": tier,
            "charged_credits": charged_credits,
            "dry_run": dry_run,
            "weekly_credits": weekly_credits,
            "used_credits": used_credits_before,
            "remaining_credits": remaining_before,
            "week_start_at": wallet["week_start_at"],
            "next_reset_at": wallet["week_end_at"],
        }

    used_credits_after = used_credits_before + charged_credits
    remaining_after = max(weekly_credits - used_credits_after, 0)

    if not dry_run:
        await conn.execute(
            """
            UPDATE user_credit_wallets
            SET used_credits = $2,
                updated_at = NOW()
            WHERE discord_uuid = $1
            """,
            discord_uuid,
            used_credits_after,
        )

        await insert_credit_usage_ledger(
            conn,
            discord_uuid=discord_uuid,
            command=normalized_command,
            tier=tier,
            charged_credits=charged_credits,
            week_start_at=wallet["week_start_at"],
            metadata=metadata,
        )

    return {
        "allowed": True,
        "reason": None,
        "discord_uuid": discord_uuid,
        "command": normalized_command,
        "tier": tier,
        "charged_credits": charged_credits,
        "dry_run": dry_run,
        "weekly_credits": weekly_credits,
        "used_credits": used_credits_before if dry_run else used_credits_after,
        "remaining_credits": remaining_before if dry_run else remaining_after,
        "week_start_at": wallet["week_start_at"],
        "next_reset_at": wallet["week_end_at"],
    }
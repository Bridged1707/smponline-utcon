from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import asyncpg
from datetime import datetime, timedelta

router = APIRouter()

class ConsumeRequest(BaseModel):
    discord_uuid: str
    command: str


WEEKLY_FREE_CREDITS = 200


async def _get_user_tier(conn, discord_uuid: str) -> str:
    row = await conn.fetchrow("""
        SELECT tier
        FROM memberships
        WHERE discord_uuid = $1
        AND is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
    """, discord_uuid)

    if not row:
        return "FREE"

    return row["tier"].upper()


async def _reset_if_needed(conn, discord_uuid: str):
    row = await conn.fetchrow("""
        SELECT credits_remaining, last_reset
        FROM user_credits
        WHERE discord_uuid = $1
    """, discord_uuid)

    now = datetime.utcnow()

    if not row:
        await conn.execute("""
            INSERT INTO user_credits (discord_uuid, credits_remaining, last_reset)
            VALUES ($1, $2, $3)
        """, discord_uuid, WEEKLY_FREE_CREDITS, now)
        return WEEKLY_FREE_CREDITS

    last_reset = row["last_reset"]

    if now - last_reset >= timedelta(days=7):
        await conn.execute("""
            UPDATE user_credits
            SET credits_remaining = $2,
                last_reset = $3
            WHERE discord_uuid = $1
        """, discord_uuid, WEEKLY_FREE_CREDITS, now)
        return WEEKLY_FREE_CREDITS

    return row["credits_remaining"]


@router.post("")
async def consume_credits(req: ConsumeRequest):
    conn: asyncpg.Connection = router.db

    tier = await _get_user_tier(conn, req.discord_uuid)

    rule = await conn.fetchrow("""
        SELECT cost
        FROM credit_rules
        WHERE command = $1 AND tier = $2
    """, req.command, tier)

    cost = rule["cost"] if rule else 0

    if cost == 0:
        return {"allowed": True}

    credits = await _reset_if_needed(conn, req.discord_uuid)

    if credits < cost:
        return {
            "allowed": False,
            "reason": "NOT_ENOUGH_CREDITS"
        }

    await conn.execute("""
        UPDATE user_credits
        SET credits_remaining = credits_remaining - $2
        WHERE discord_uuid = $1
    """, req.discord_uuid, cost)

    return {"allowed": True}
# utils.py
import json
from typing import Dict, Any
from ._db import get_pool
import asyncpg

DEFAULT_RATES = {
    "deposit_multiplier": 1.0,
    "withdraw_multiplier": 1.0,
    "transfer_fee_percent": 0.0,
    "transfer_fee_flat": 0.0
}

async def get_account_and_rates(discord_uuid: str) -> Dict[str, Any]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT a.discord_uuid, a.mc_uuid, a.mc_name, a.roles, a.rates, b.balance
            FROM accounts a
            LEFT JOIN balances b ON b.discord_uuid = a.discord_uuid
            WHERE a.discord_uuid = $1
        """, discord_uuid)
        if not row:
            # Not found in accounts - we still want to allow operations on balances only?
            # We'll return with default roles/rates and balance if a balance row exists.
            bal = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1", discord_uuid)
            if bal:
                return {"discord_uuid": discord_uuid, "roles": [], "rates": DEFAULT_RATES.copy(), "balance": float(bal["balance"])}
            raise asyncpg.exceptions.PostgresError(f"Account with discord_uuid={discord_uuid} not found")

        rates = row["rates"] or DEFAULT_RATES.copy()
        # Make sure numeric fields exist and are floats
        for k,v in DEFAULT_RATES.items():
            if k not in rates:
                rates[k] = v
        return {
            "discord_uuid": row["discord_uuid"],
            "mc_uuid": row["mc_uuid"],
            "mc_name": row["mc_name"],
            "roles": row["roles"] or [],
            "rates": rates,
            "balance": float(row["balance"] or 0)
        }

async def ensure_balance_row(conn, discord_uuid: str):
    await conn.execute("""
        INSERT INTO balances (discord_uuid, balance)
        VALUES ($1, 0)
        ON CONFLICT (discord_uuid) DO NOTHING
    """, discord_uuid)
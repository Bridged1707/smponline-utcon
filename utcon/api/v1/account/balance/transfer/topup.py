# topup.py
from fastapi import APIRouter, HTTPException, Depends
from ..schemas import TopupRequest
from .._db import get_pool
from ..utils import ensure_balance_row
import decimal, json

router = APIRouter()

# NOTE: protect this route in your real environment (admin auth). For now it is open.
@router.post("/v1/account/balance/transfer/topup")
async def topup(req: TopupRequest):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_balance_row(conn, req.discord_uuid)
            amt = decimal.Decimal(str(req.amount))
            await conn.execute("UPDATE balances SET balance = balance + $1, last_updated = now() WHERE discord_uuid = $2", amt, req.discord_uuid)
            await conn.execute("""
                INSERT INTO balance_transactions (discord_uuid, kind, amount, metadata)
                VALUES ($1, 'topup', $2, $3::jsonb)
            """, req.discord_uuid, amt, json.dumps({"operator": req.operator, "reason": req.reason}))
            row = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1", req.discord_uuid)
            return {"discord_uuid": req.discord_uuid, "new_balance": float(row["balance"])}
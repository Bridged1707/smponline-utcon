# deposit.py
from fastapi import APIRouter, HTTPException
from ..schemas import TransferRequest
from .._db import get_pool
from ..utils import get_account_and_rates, ensure_balance_row
import decimal, json

router = APIRouter()

@router.post("/v1/account/balance/transfer/deposit")
async def deposit(req: TransferRequest):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Ensure account exists + get rates
            acc = await get_account_and_rates(req.discord_uuid)
            await ensure_balance_row(conn, req.discord_uuid)

            rates = acc["rates"]
            deposit_multiplier = float(rates.get("deposit_multiplier", 1.0))
            applied_amount = decimal.Decimal(str(req.amount)) * decimal.Decimal(str(deposit_multiplier))

            await conn.execute("""
                UPDATE balances SET balance = balance + $1, last_updated = now()
                WHERE discord_uuid = $2
            """, applied_amount, req.discord_uuid)

            await conn.execute("""
                INSERT INTO balance_transactions
                (discord_uuid, kind, amount, applied_rates, metadata)
                VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
            """, req.discord_uuid, "deposit", applied_amount, json.dumps(rates), json.dumps({"operator": req.operator, "reason": req.reason}))

            row = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1", req.discord_uuid)
            return {"discord_uuid": req.discord_uuid, "new_balance": float(row["balance"])}
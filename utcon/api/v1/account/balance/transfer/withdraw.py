# withdraw.py
from fastapi import APIRouter, HTTPException
from .schemas import TransferRequest
from ._db import get_pool
from .utils import get_account_and_rates
import decimal, json

router = APIRouter()

@router.post("/v1/account/balance/transfer/withdraw")
async def withdraw(req: TransferRequest):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            acc = await get_account_and_rates(req.discord_uuid)
            await conn.execute("SELECT 1 FROM balances WHERE discord_uuid=$1", req.discord_uuid)  # ensure exists
            rates = acc["rates"]
            withdraw_multiplier = float(rates.get("withdraw_multiplier", 1.0))
            amount_decimal = decimal.Decimal(str(req.amount))
            deduction = amount_decimal * decimal.Decimal(str(withdraw_multiplier))

            # check balance
            row = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1 FOR UPDATE", req.discord_uuid)
            if not row:
                raise HTTPException(status_code=404, detail="Balance row not found")
            current = decimal.Decimal(str(row["balance"]))
            if current < deduction:
                raise HTTPException(status_code=400, detail="Insufficient funds")

            await conn.execute("UPDATE balances SET balance = balance - $1, last_updated = now() WHERE discord_uuid = $2", deduction, req.discord_uuid)
            await conn.execute("""
                INSERT INTO balance_transactions
                (discord_uuid, kind, amount, applied_rates, metadata)
                VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
            """, req.discord_uuid, "withdraw", -deduction, json.dumps(rates), json.dumps({"operator": req.operator, "reason": req.reason}))
            newrow = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1", req.discord_uuid)
            return {"discord_uuid": req.discord_uuid, "new_balance": float(newrow["balance"])}
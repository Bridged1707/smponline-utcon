# transfer.py
from fastapi import APIRouter, HTTPException
from ..schemas import TransferBetweenRequest
from .._db import get_pool
from ..utils import get_account_and_rates, ensure_balance_row
import decimal, json

router = APIRouter()

@router.post("/v1/account/balance/transfer/transfer")
async def transfer(req: TransferBetweenRequest):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Load source and destination (will raise if account not found)
            from_acc = await get_account_and_rates(req.from_discord_uuid)
            to_acc = await get_account_and_rates(req.to_discord_uuid)

            # Ensure rows exist
            await ensure_balance_row(conn, req.from_discord_uuid)
            await ensure_balance_row(conn, req.to_discord_uuid)

            # Compute amounts
            amount = decimal.Decimal(str(req.amount))

            src_rates = from_acc["rates"]
            dst_rates = to_acc["rates"]

            withdraw_multiplier = decimal.Decimal(str(src_rates.get("withdraw_multiplier", 1.0)))
            fee_percent = decimal.Decimal(str(src_rates.get("transfer_fee_percent", 0.0)))
            fee_flat = decimal.Decimal(str(src_rates.get("transfer_fee_flat", 0.0)))

            src_deduction = amount * withdraw_multiplier + (amount * fee_percent) + fee_flat
            dst_credit = amount * decimal.Decimal(str(dst_rates.get("deposit_multiplier", 1.0)))

            # Lock balances
            src_row = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1 FOR UPDATE", req.from_discord_uuid)
            dst_row = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1 FOR UPDATE", req.to_discord_uuid)

            if src_row is None or dst_row is None:
                raise HTTPException(status_code=404, detail="Balance row not found for one of the accounts")

            src_balance = decimal.Decimal(str(src_row["balance"]))
            if src_balance < src_deduction:
                raise HTTPException(status_code=400, detail="Insufficient funds")

            # Apply updates
            await conn.execute("UPDATE balances SET balance = balance - $1, last_updated = now() WHERE discord_uuid = $2", src_deduction, req.from_discord_uuid)
            await conn.execute("UPDATE balances SET balance = balance + $1, last_updated = now() WHERE discord_uuid = $2", dst_credit, req.to_discord_uuid)

            # Insert audit rows (two rows)
            await conn.execute("""
                INSERT INTO balance_transactions
                (discord_uuid, kind, amount, related_discord_uuid, applied_rates, metadata)
                VALUES ($1, 'transfer_out', $2, $3, $4::jsonb, $5::jsonb)
            """, req.from_discord_uuid, -src_deduction, req.to_discord_uuid, json.dumps(src_rates), json.dumps({"operator": req.operator, "reason": req.reason}))

            await conn.execute("""
                INSERT INTO balance_transactions
                (discord_uuid, kind, amount, related_discord_uuid, applied_rates, metadata)
                VALUES ($1, 'transfer_in', $2, $3, $4::jsonb, $5::jsonb)
            """, req.to_discord_uuid, dst_credit, req.from_discord_uuid, json.dumps(dst_rates), json.dumps({"operator": req.operator, "reason": req.reason}))

            new_src = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1", req.from_discord_uuid)
            new_dst = await conn.fetchrow("SELECT balance FROM balances WHERE discord_uuid = $1", req.to_discord_uuid)

            return {
                "from_discord_uuid": req.from_discord_uuid,
                "to_discord_uuid": req.to_discord_uuid,
                "amount_requested": float(amount),
                "debit_from_source": float(src_deduction),
                "credit_to_destination": float(dst_credit),
                "new_source_balance": float(new_src["balance"]),
                "new_destination_balance": float(new_dst["balance"])
            }
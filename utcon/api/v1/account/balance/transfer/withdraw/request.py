from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from decimal import Decimal

from utcon.database import get_pool
from utcon.repositories import balance as balance_repo

router = APIRouter(
    prefix="/v1/account/balance/transfer/withdraw",
    tags=["balance"]
)


class WithdrawRequest(BaseModel):
    discord_uuid: str
    amount: Decimal


@router.post("/request")
async def request_withdrawal(req: WithdrawRequest):
    pool = get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():

            balance = await balance_repo.get_balance(conn, req.discord_uuid)

            if balance is None:
                raise HTTPException(status_code=404, detail="Account not found")

            if balance < req.amount:
                raise HTTPException(status_code=400, detail="Insufficient balance")

            # Deduct immediately
            new_balance = balance - req.amount
            await balance_repo.update_balance(conn, req.discord_uuid, new_balance)

            # Insert into withdraw queue
            row = await conn.fetchrow(
                """
                INSERT INTO withdraw_queue (discord_uuid, amount, status, requested_at)
                VALUES ($1, $2, 'pending', NOW())
                RETURNING *
                """,
                req.discord_uuid,
                req.amount
            )

            # 🔥 FIX IS RIGHT HERE
            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=req.discord_uuid,
                kind="withdraw",  # <-- MUST MATCH CHECK CONSTRAINT
                amount=-req.amount,
                metadata={"queue_id": row["id"]}
            )

    return {
        "status": "withdraw_queued",
        "queue": dict(row),
        "balance": new_balance
    }
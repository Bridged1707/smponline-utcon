from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from utcon import db
from utcon.repositories import balance as balance_repo

router = APIRouter(prefix="/v1/account/balance/transfer/withdraw", tags=["balance"])


class WithdrawRequest(BaseModel):
    discord_uuid: str
    amount: Decimal


@router.post("/request")
async def request_withdrawal(req: WithdrawRequest):
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than zero")

    async with db.connection() as conn:
        async with conn.transaction():
            balance = await balance_repo.get_balance_for_update(conn, req.discord_uuid)

            if balance is None:
                raise HTTPException(status_code=404, detail="Account not found")

            if balance < req.amount:
                raise HTTPException(status_code=400, detail="Insufficient balance")

            await balance_repo.subtract_balance(conn, req.discord_uuid, req.amount)

            row = await conn.fetchrow(
                """
                INSERT INTO withdraw_queue (
                    discord_uuid,
                    amount,
                    status,
                    requested_at
                )
                VALUES ($1, $2, 'pending', NOW())
                RETURNING id, discord_uuid, amount, status, requested_at, processed_at, processed_by, notes, reason
                """,
                req.discord_uuid,
                req.amount,
            )

            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=req.discord_uuid,
                kind="withdraw",
                amount=-req.amount,
                metadata={"queue_id": row["id"]},
            )

            new_balance = balance - req.amount

    return {
        "status": "withdraw_queued",
        "queue": dict(row),
        "balance": new_balance,
    }

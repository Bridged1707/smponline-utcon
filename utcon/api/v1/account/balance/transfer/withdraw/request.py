from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from utcon import db
from utcon.repositories import balance as balance_repo

router = APIRouter(prefix="/v1/account/balance/withdraw", tags=["balance"])


class WithdrawRequest(BaseModel):
    discord_uuid: str
    amount: float
    reason: str | None = None


@router.post("/request")
async def request_withdrawal(req: WithdrawRequest):
    amount = Decimal(str(req.amount))

    if amount <= 0:
        raise HTTPException(status_code=400, detail="invalid_amount")

    async with db.connection() as conn:
        async with conn.transaction():
            balance = await balance_repo.get_balance_for_update(conn, req.discord_uuid)
            if balance is None:
                raise HTTPException(status_code=404, detail="account_balance_not_found")

            if balance < amount:
                raise HTTPException(status_code=400, detail="insufficient_balance")

            await balance_repo.subtract_balance(conn, req.discord_uuid, amount)

            queue_row = await conn.fetchrow(
                """
                INSERT INTO withdraw_queue (
                    discord_uuid,
                    amount,
                    status,
                    requested_at,
                    reason
                )
                VALUES ($1, $2, 'pending', NOW(), $3)
                RETURNING id, discord_uuid, amount, status, requested_at, processed_at, processed_by, notes, reason
                """,
                req.discord_uuid,
                amount,
                req.reason,
            )

            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=req.discord_uuid,
                kind="withdraw_request",
                amount=amount,
                metadata={"queue_id": int(queue_row["id"]), "reason": req.reason} if req.reason else {"queue_id": int(queue_row["id"])} ,
            )

            new_balance = await balance_repo.get_balance(conn, req.discord_uuid)

    return {
        "status": "withdraw_queued",
        "queue": {
            "id": int(queue_row["id"]),
            "discord_uuid": queue_row["discord_uuid"],
            "amount": float(queue_row["amount"]),
            "status": queue_row["status"],
            "requested_at": queue_row["requested_at"],
            "processed_at": queue_row["processed_at"],
            "processed_by": queue_row["processed_by"],
            "notes": queue_row["notes"],
            "reason": queue_row["reason"],
        },
        "balance": float(new_balance) if new_balance is not None else None,
    }

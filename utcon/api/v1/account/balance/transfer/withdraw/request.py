from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import balance as balance_repo
from utcon.schemas.balance import BalanceRequest

router = APIRouter(prefix="/v1/account/balance/transfer/withdraw", tags=["balance"])


@router.post("/request")
async def request_withdrawal(req: BalanceRequest):
    amount = Decimal(str(req.amount))

    if amount <= 0:
        raise HTTPException(status_code=400, detail="invalid_amount")

    async with db.connection() as conn:
        async with conn.transaction():
            balance = await balance_repo.get_balance_for_update(conn, req.discord_uuid)

            if balance is None:
                raise HTTPException(status_code=404, detail="account_not_found")

            if balance < amount:
                raise HTTPException(status_code=400, detail="insufficient_balance")

            await balance_repo.subtract_balance(conn, req.discord_uuid, amount)

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
                amount,
            )

            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=req.discord_uuid,
                kind="withdraw",
                amount=amount,
                metadata={"queue_id": row["id"]},
            )

            new_balance = await balance_repo.get_balance(conn, req.discord_uuid)

    queue = dict(row)
    queue["amount"] = float(queue["amount"])
    queue["requested_at"] = queue["requested_at"].isoformat() if queue["requested_at"] else None
    queue["processed_at"] = queue["processed_at"].isoformat() if queue["processed_at"] else None

    return {
        "status": "withdraw_queued",
        "amount": float(amount),
        "queue": queue,
        "balance": float(new_balance) if new_balance is not None else None,
    }

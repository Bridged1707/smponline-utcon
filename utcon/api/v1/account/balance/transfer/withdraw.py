from fastapi import APIRouter, HTTPException
from utcon import db
from ..schemas import WithdrawRequest

router = APIRouter()


@router.post("/account/balance/transfer/withdraw")
async def withdraw(req: WithdrawRequest):

    async with db.connection() as conn:

        row = await conn.fetchrow(
            "SELECT balance FROM balances WHERE discord_uuid=$1",
            req.discord_uuid
        )

        if row is None:
            raise HTTPException(404, "Account not found")

        if row["balance"] < req.amount:
            raise HTTPException(400, "Insufficient balance")

        queue_id = await conn.fetchval(
            """
            INSERT INTO withdraw_queue (discord_uuid, amount)
            VALUES ($1,$2)
            RETURNING id
            """,
            req.discord_uuid,
            req.amount
        )

        return {
            "status": "queued",
            "queue_id": queue_id
        }
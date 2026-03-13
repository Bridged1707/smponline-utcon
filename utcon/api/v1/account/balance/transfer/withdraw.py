from fastapi import APIRouter, HTTPException
from utcon import db
from ..schemas import WithdrawRequest

router = APIRouter()


@router.post("/account/balance/transfer/withdraw")
async def withdraw(req: WithdrawRequest):

    async with db.connection() as conn:

        row = await conn.fetchrow(
            "SELECT balance FROM balances WHERE discord_uuid=$1 FOR UPDATE",
            req.discord_uuid
        )

        if row is None:
            raise HTTPException(status_code=404, detail="Account not found")

        if row["balance"] < req.amount:
            raise HTTPException(status_code=400, detail="Insufficient funds")

        await conn.execute(
            """
            UPDATE balances
            SET balance = balance - $1
            WHERE discord_uuid = $2
            """,
            req.amount,
            req.discord_uuid
        )

        new_row = await conn.fetchrow(
            "SELECT balance FROM balances WHERE discord_uuid=$1",
            req.discord_uuid
        )

        return {
            "discord_uuid": req.discord_uuid,
            "new_balance": float(new_row["balance"])
        }
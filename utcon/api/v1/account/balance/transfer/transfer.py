from fastapi import APIRouter, HTTPException
from utcon import db
from ..schemas import TransferRequest

router = APIRouter()


@router.post("/account/balance/transfer/transfer")
async def transfer(req: TransferRequest):

    async with db.connection() as conn:

        sender = await conn.fetchrow(
            "SELECT balance FROM balances WHERE discord_uuid=$1 FOR UPDATE",
            req.from_discord_uuid
        )

        if sender is None:
            raise HTTPException(status_code=404, detail="Sender not found")

        if sender["balance"] < req.amount:
            raise HTTPException(status_code=400, detail="Insufficient funds")

        await conn.execute(
            """
            UPDATE balances
            SET balance = balance - $1
            WHERE discord_uuid=$2
            """,
            req.amount,
            req.from_discord_uuid
        )

        await conn.execute(
            """
            INSERT INTO balances (discord_uuid,balance)
            VALUES ($1,$2)
            ON CONFLICT (discord_uuid)
            DO UPDATE SET balance = balances.balance + $2
            """,
            req.to_discord_uuid,
            req.amount
        )

        return {
            "from": req.from_discord_uuid,
            "to": req.to_discord_uuid,
            "amount": req.amount
        }
from fastapi import APIRouter
from utcon import db
from ..schemas import TopupRequest

router = APIRouter()


@router.post("/account/balance/transfer/topup")
async def topup(req: TopupRequest):

    async with db.connection() as conn:

        await conn.execute(
            """
            INSERT INTO balances (discord_uuid, balance)
            VALUES ($1,$2)
            ON CONFLICT (discord_uuid)
            DO UPDATE SET balance = balances.balance + $2
            """,
            req.discord_uuid,
            req.amount
        )

        row = await conn.fetchrow(
            "SELECT balance FROM balances WHERE discord_uuid=$1",
            req.discord_uuid
        )

        return {
            "discord_uuid": req.discord_uuid,
            "new_balance": float(row["balance"])
        }
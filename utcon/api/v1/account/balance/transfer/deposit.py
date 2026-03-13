from fastapi import APIRouter
from pydantic import BaseModel
import utcon.db as db

router = APIRouter()


class DepositRequest(BaseModel):
    discord_uuid: str
    amount: float


@router.post("/v1/account/balance/transfer/deposit")
async def deposit(req: DepositRequest):

    async with db.connection() as conn:

        await conn.execute(
            """
            INSERT INTO balance_transfers
            (discord_uuid, type, amount, status)
            VALUES ($1,$2,$3,$4)
            """,
            req.discord_uuid,
            "deposit",
            req.amount,
            "completed"
        )

    return {"status": "success"}
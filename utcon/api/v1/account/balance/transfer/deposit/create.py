from fastapi import APIRouter
from utcon import db
from utcon.schemas.balance import BalanceRequest

router = APIRouter(prefix="/v1/account/balance/transfer/deposit", tags=["balance"])


@router.post("")
async def deposit(req: BalanceRequest):

    async with db.connection() as conn:

        await conn.execute(
            """
            INSERT INTO balance_transfers (
                discord_uuid,
                type,
                amount,
                status
            )
            VALUES ($1,$2,$3,$4)
            """,
            req.discord_uuid,
            "deposit",
            req.amount,
            "completed"
        )

    return {"status": "deposit_complete"}
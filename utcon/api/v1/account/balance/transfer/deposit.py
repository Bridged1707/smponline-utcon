from fastapi import APIRouter

from utcon import db
from utcon.schemas.balance import BalanceRequest
from utcon.repositories import balance as balance_repo

router = APIRouter(prefix="/v1/account/balance/transfer", tags=["balance"])


@router.post("/deposit")
async def deposit(req: BalanceRequest):

    async with db.connection() as conn:

        await balance_repo.add_balance(
            conn,
            req.discord_uuid,
            req.amount,
        )

    return {"status": "deposit_complete"}
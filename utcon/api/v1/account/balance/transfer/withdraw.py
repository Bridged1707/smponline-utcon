from fastapi import APIRouter, HTTPException
from utcon import db
from utcon.schemas.balance import BalanceRequest
from utcon.repositories import balance as balance_repo

router = APIRouter(prefix="/v1/account/balance/transfer", tags=["balance"])


@router.post("/withdraw")
async def withdraw(req: BalanceRequest):

    async with db.connection() as conn:

        balance = await balance_repo.get_balance(
            conn,
            req.discord_uuid
        )

        if balance < req.amount:
            raise HTTPException(status_code=400, detail="Insufficient funds")

        await balance_repo.subtract_balance(
            conn,
            req.discord_uuid,
            req.amount
        )

        await conn.execute(
            """
            INSERT INTO balance_transfers(
                type,
                from_discord_uuid,
                amount,
                status
            )
            VALUES ('withdraw',$1,$2,'completed')
            """,
            req.discord_uuid,
            req.amount
        )

    return {"status": "withdraw_complete"}
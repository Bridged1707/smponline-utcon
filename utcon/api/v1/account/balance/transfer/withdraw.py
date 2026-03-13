from fastapi import APIRouter, HTTPException
from utcon import db
from utcon.schemas.balance import BalanceRequest
from utcon.repositories import balance as balance_repo

router = APIRouter(prefix="/v1/account/balance/transfer", tags=["balance"])


@router.post("/withdraw")
async def withdraw(req: BalanceRequest):

    async with db.connection() as conn:

        # lock account balance to prevent race conditions
        async with conn.transaction():

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
                INSERT INTO balance_transfers (
                    discord_uuid,
                    type,
                    from_discord_uuid,
                    amount,
                    status
                )
                VALUES ($1,$2,$3,$4,$5)
                """,
                req.discord_uuid,
                "withdraw",
                req.discord_uuid,
                req.amount,
                "completed"
            )

    return {"status": "withdraw_complete"}
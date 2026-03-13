from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from utcon import db
from utcon.repositories import balance as balance_repo

router = APIRouter(prefix="/v1/account/balance/transfer", tags=["balance"])


class PayRequest(BaseModel):

    from_discord_uuid: str
    to_discord_uuid: str
    amount: float


@router.post("/pay")
async def pay(req: PayRequest):

    if req.from_discord_uuid == req.to_discord_uuid:
        raise HTTPException(status_code=400, detail="Cannot pay yourself")

    async with db.connection() as conn:

        sender_balance = await balance_repo.get_balance(
            conn,
            req.from_discord_uuid
        )

        if sender_balance < req.amount:
            raise HTTPException(status_code=400, detail="Insufficient funds")

        # subtract sender
        await balance_repo.subtract_balance(
            conn,
            req.from_discord_uuid,
            req.amount
        )

        # add receiver
        await balance_repo.add_balance(
            conn,
            req.to_discord_uuid,
            req.amount
        )

        # ledger entry
        await conn.execute(
            """
            INSERT INTO balance_transfers(
                type,
                from_discord_uuid,
                to_discord_uuid,
                amount,
                status
            )
            VALUES ('pay',$1,$2,$3,'completed')
            """,
            req.from_discord_uuid,
            req.to_discord_uuid,
            req.amount
        )

    return {"status": "payment_complete"}
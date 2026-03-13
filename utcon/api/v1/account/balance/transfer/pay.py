from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from decimal import Decimal
from utcon.db import get_pool

router = APIRouter()

class PayRequest(BaseModel):
    from_discord_uuid: str
    to_discord_uuid: str
    amount: Decimal


@router.post("/v1/account/balance/transfer/pay")
async def pay(req: PayRequest):

    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="invalid_amount")

    if req.from_discord_uuid == req.to_discord_uuid:
        raise HTTPException(status_code=400, detail="cannot_pay_self")

    pool = get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():

            sender = await conn.fetchrow(
                "SELECT balance FROM balances WHERE discord_uuid=$1 FOR UPDATE",
                req.from_discord_uuid
            )

            if not sender:
                raise HTTPException(status_code=404, detail="sender_not_found")

            if sender["balance"] < req.amount:
                raise HTTPException(status_code=400, detail="insufficient_balance")

            receiver = await conn.fetchrow(
                "SELECT balance FROM balances WHERE discord_uuid=$1 FOR UPDATE",
                req.to_discord_uuid
            )

            if not receiver:
                raise HTTPException(status_code=404, detail="receiver_not_found")

            await conn.execute(
                "UPDATE balances SET balance = balance - $1 WHERE discord_uuid=$2",
                req.amount,
                req.from_discord_uuid
            )

            await conn.execute(
                "UPDATE balances SET balance = balance + $1 WHERE discord_uuid=$2",
                req.amount,
                req.to_discord_uuid
            )

            # sender ledger
            await conn.execute(
                """
                INSERT INTO balance_transfers
                (discord_uuid, type, amount, status, from_discord_uuid, to_discord_uuid)
                VALUES ($1,'pay_out',$2,'completed',$1,$3)
                """,
                req.from_discord_uuid,
                req.amount,
                req.to_discord_uuid
            )

            # receiver ledger
            await conn.execute(
                """
                INSERT INTO balance_transfers
                (discord_uuid, type, amount, status, from_discord_uuid, to_discord_uuid)
                VALUES ($1,'pay_in',$2,'completed',$3,$1)
                """,
                req.to_discord_uuid,
                req.amount,
                req.from_discord_uuid
            )

    return {"status": "payment_complete"}
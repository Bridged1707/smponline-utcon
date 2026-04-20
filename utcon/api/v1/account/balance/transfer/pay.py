from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from decimal import Decimal

from utcon import db
from utcon.repositories import balance as balance_repo
from utcon.repositories import balance_notifications as balance_notifications_repo

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

    async with db.connection() as conn:
        async with conn.transaction():
            sender = await conn.fetchrow(
                "SELECT balance FROM balances WHERE discord_uuid=$1 FOR UPDATE",
                req.from_discord_uuid,
            )
            if sender is None:
                raise HTTPException(status_code=404, detail="sender_not_found")

            receiver = await conn.fetchrow(
                "SELECT balance FROM balances WHERE discord_uuid=$1 FOR UPDATE",
                req.to_discord_uuid,
            )
            if receiver is None:
                raise HTTPException(status_code=404, detail="receiver_not_found")

            if sender["balance"] < req.amount:
                raise HTTPException(status_code=400, detail="insufficient_balance")

            await balance_repo.subtract_balance(conn, req.from_discord_uuid, req.amount)
            await balance_repo.add_balance(conn, req.to_discord_uuid, req.amount)

            await conn.execute(
                """
                INSERT INTO balance_transfers(discord_uuid, type, amount, status, from_discord_uuid, to_discord_uuid)
                VALUES($1,$2,$3,$4,$5,$6)
                """,
                req.from_discord_uuid,
                "pay",
                req.amount,
                "completed",
                req.from_discord_uuid,
                req.to_discord_uuid,
            )

            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=req.from_discord_uuid,
                kind="pay",
                amount=req.amount,
                related_discord_uuid=req.to_discord_uuid,
            )

            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=req.to_discord_uuid,
                kind="pay",
                amount=req.amount,
                related_discord_uuid=req.from_discord_uuid,
                metadata={"direction": "inbound"},
            )

            await balance_notifications_repo.create_balance_notification(
                conn,
                discord_uuid=req.to_discord_uuid,
                amount=req.amount,
                reason="Payment received",
                source="pay",
                metadata={
                    "from_discord_uuid": req.from_discord_uuid,
                    "to_discord_uuid": req.to_discord_uuid,
                },
            )

            sender_after = await conn.fetchval(
                "SELECT balance FROM balances WHERE discord_uuid=$1",
                req.from_discord_uuid,
            )
            receiver_after = await conn.fetchval(
                "SELECT balance FROM balances WHERE discord_uuid=$1",
                req.to_discord_uuid,
            )

    return {
        "status": "payment_complete",
        "amount": req.amount,
        "from_discord_uuid": req.from_discord_uuid,
        "to_discord_uuid": req.to_discord_uuid,
        "from_balance": sender_after,
        "to_balance": receiver_after,
        "new_balance": receiver_after,
    }
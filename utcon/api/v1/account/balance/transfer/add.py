from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import balance as balance_repo
from utcon.schemas.balance import AdminBalanceAdjustRequest

router = APIRouter(prefix="/v1/account/balance/transfer", tags=["balance"])


@router.post("/add")
async def add_balance(req: AdminBalanceAdjustRequest):
    amount = Decimal(str(req.amount))

    if amount <= 0:
        raise HTTPException(status_code=400, detail="invalid_amount")

    async with db.connection() as conn:
        async with conn.transaction():
            existing = await balance_repo.get_balance_for_update(conn, req.discord_uuid)
            if existing is None:
                raise HTTPException(status_code=404, detail="account_balance_not_found")

            await balance_repo.add_balance(conn, req.discord_uuid, amount)

            await conn.execute(
                """
                INSERT INTO balance_transfers (
                    discord_uuid,
                    type,
                    amount,
                    status,
                    reference,
                    to_discord_uuid
                )
                VALUES ($1, $2, $3, $4, $5, $1)
                """,
                req.discord_uuid,
                "add",
                amount,
                "completed",
                req.reference,
            )

            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=req.discord_uuid,
                kind="admin_add",
                amount=amount,
                metadata={"reference": req.reference} if req.reference else None,
            )

            new_balance = await balance_repo.get_balance(conn, req.discord_uuid)

    return {
        "status": "balance_added",
        "discord_uuid": req.discord_uuid,
        "amount": float(amount),
        "balance": float(new_balance),
    }
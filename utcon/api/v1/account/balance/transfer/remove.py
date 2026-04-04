from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import balance as balance_repo
from utcon.schemas.balance import AdminBalanceAdjustRequest

router = APIRouter(prefix="/v1/account/balance/transfer", tags=["balance"])


@router.post("/remove")
async def remove_balance(req: AdminBalanceAdjustRequest):
    amount = Decimal(str(req.amount))

    if amount <= 0:
        raise HTTPException(status_code=400, detail="invalid_amount")

    async with db.connection() as conn:
        async with conn.transaction():
            existing = await balance_repo.get_balance_for_update(conn, req.discord_uuid)
            if existing is None:
                raise HTTPException(status_code=404, detail="account_balance_not_found")

            if existing < amount:
                raise HTTPException(status_code=400, detail="insufficient_balance")

            await balance_repo.subtract_balance(conn, req.discord_uuid, amount)

            await conn.execute(
                """
                INSERT INTO balance_transfers (
                    discord_uuid,
                    type,
                    amount,
                    status,
                    reference,
                    from_discord_uuid
                )
                VALUES ($1, $2, $3, $4, $5, $1)
                """,
                req.discord_uuid,
                "remove",
                amount,
                "completed",
                req.reference,
            )

            # Use admin_add with a negative amount for compatibility with
            # current balance_transactions kind constraints.
            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=req.discord_uuid,
                kind="admin_add",
                amount=-amount,
                metadata=(
                    {
                        "reference": req.reference,
                        "admin_action": "remove",
                    }
                    if req.reference
                    else {
                        "admin_action": "remove",
                    }
                ),
            )

            new_balance = await balance_repo.get_balance(conn, req.discord_uuid)

    return {
        "status": "balance_removed",
        "discord_uuid": req.discord_uuid,
        "amount": float(amount),
        "balance": float(new_balance),
    }
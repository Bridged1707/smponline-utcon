from fastapi import APIRouter
from utcon import db

router = APIRouter(
    prefix="/v1/admin/account/balance/transfer/withdraw",
    tags=["admin"]
)


@router.get("/pending")
async def pending_withdrawals():

    async with db.connection() as conn:

        rows = await conn.fetch(
            """
            SELECT id, discord_uuid, amount, created_at
            FROM balance_transfers
            WHERE type='withdraw'
            AND status='pending'
            ORDER BY created_at
            """
        )

    return [dict(r) for r in rows]
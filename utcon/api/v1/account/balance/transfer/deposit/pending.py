from fastapi import APIRouter
from utcon import db

router = APIRouter(prefix="/v1/admin/account/balance/transfer/deposit")

@router.get("/pending")
async def get_pending_deposits():

    async with db.connection() as conn:

        rows = await conn.fetch(
            """
            SELECT id, discord_uuid, amount, created_at
            FROM balance_transfers
            WHERE type='deposit'
            AND status='pending'
            ORDER BY created_at ASC
            """
        )

    return [dict(r) for r in rows]
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
            SELECT id, discord_uuid, amount, status, requested_at, processed_at, processed_by, notes, reason
            FROM withdraw_queue
            WHERE status='pending'
            ORDER BY requested_at
            """
        )

    payload = []
    for row in rows:
        item = dict(row)
        item["amount"] = float(item["amount"])
        item["requested_at"] = item["requested_at"].isoformat() if item["requested_at"] else None
        item["processed_at"] = item["processed_at"].isoformat() if item["processed_at"] else None
        payload.append(item)

    return payload

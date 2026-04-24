from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.schemas.withdraw import WithdrawResolveRequest

router = APIRouter(prefix="/v1/admin/account/balance/transfer/withdraw", tags=["admin"])


@router.post("/reject")
async def reject_withdrawal(req: WithdrawResolveRequest):
    withdrawal_id = req.effective_withdrawal_id

    async with db.connection() as conn:
        async with conn.transaction():
            if withdrawal_id is not None:
                row = await conn.fetchrow(
                    """
                    SELECT id, discord_uuid, amount, status, requested_at, processed_at, processed_by, notes, reason
                    FROM withdraw_queue
                    WHERE id = $1
                      AND discord_uuid = $2
                      AND status = 'pending'
                    FOR UPDATE
                    """,
                    withdrawal_id,
                    req.discord_uuid,
                )
            else:
                row = await conn.fetchrow(
                    """
                    SELECT id, discord_uuid, amount, status, requested_at, processed_at, processed_by, notes, reason
                    FROM withdraw_queue
                    WHERE discord_uuid = $1
                      AND status = 'pending'
                    ORDER BY requested_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    req.discord_uuid,
                )

            if row is None:
                raise HTTPException(status_code=404, detail="withdraw_pending_not_found")

            updated = await conn.fetchrow(
                """
                UPDATE withdraw_queue
                SET status = 'rejected',
                    processed_at = NOW(),
                    processed_by = $2,
                    notes = COALESCE($3, notes)
                WHERE id = $1
                RETURNING id, discord_uuid, amount, status, requested_at, processed_at, processed_by, notes, reason
                """,
                row["id"],
                req.processed_by or "admin",
                req.notes,
            )

    queue = _serialize_queue_item(updated)

    return {
        "status": "withdraw_rejected",
        "queue": queue,
    }


def _serialize_queue_item(item):
    payload = dict(item)
    payload["amount"] = float(payload["amount"])
    payload["requested_at"] = payload["requested_at"].isoformat() if payload.get("requested_at") else None
    payload["processed_at"] = payload["processed_at"].isoformat() if payload.get("processed_at") else None
    return payload

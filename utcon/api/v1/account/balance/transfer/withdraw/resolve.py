from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import balance as balance_repo
from utcon.schemas.withdraw import WithdrawResolveRequest

router = APIRouter(prefix="/v1/admin/account/balance/transfer/withdraw", tags=["admin"])


@router.post("/resolve")
async def resolve_withdrawal(req: WithdrawResolveRequest):
    async with db.connection() as conn:
        async with conn.transaction():
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
                SET status = 'completed',
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

            balance = await balance_repo.get_balance(conn, req.discord_uuid)

    queue = _serialize_queue_item(updated)

    return {
        "status": "withdraw_resolved",
        "queue": queue,
        "balance": float(balance) if balance is not None else None,
    }



def _serialize_queue_item(item):
    payload = dict(item)
    payload["amount"] = float(payload["amount"])
    payload["requested_at"] = payload["requested_at"].isoformat() if payload.get("requested_at") else None
    payload["processed_at"] = payload["processed_at"].isoformat() if payload.get("processed_at") else None
    return payload

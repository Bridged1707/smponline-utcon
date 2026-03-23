from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import deposit as deposit_repo
from utcon.schemas.deposit import DepositChallengeFailRequest

router = APIRouter(prefix="/v1/account/balance/transfer/deposit", tags=["balance"])


@router.post("/fail")
async def fail_deposit(req: DepositChallengeFailRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            await deposit_repo.ensure_deposit_schema(conn)

            queue_item = await deposit_repo.mark_deposit_status(
                conn,
                queue_id=req.queue_id,
                status=req.status,
                failure_reason=req.failure_reason,
                processed_by=req.processed_by or "utmp",
            )

    if queue_item is None:
        raise HTTPException(status_code=404, detail="deposit_queue_item_not_found")

    return {
        "status": queue_item["status"],
        "queue": _serialize_queue_item(queue_item),
    }


def _serialize_queue_item(item):
    return {
        "id": item["id"],
        "discord_uuid": item["discord_uuid"],
        "challenge_shop_id": item["challenge_shop_id"],
        "challenge_owner_uuid": str(item["challenge_owner_uuid"]),
        "challenge_owner_name": item["challenge_owner_name"],
        "challenge_item_type": item["challenge_item_type"],
        "challenge_item_name": item.get("challenge_item_name"),
        "challenge_item_quantity": item["challenge_item_quantity"],
        "challenge_price": float(item["challenge_price"]),
        "expected_total": float(item["expected_total"]),
        "challenge_world": item["challenge_world"],
        "challenge_x": item["challenge_x"],
        "challenge_y": item["challenge_y"],
        "challenge_z": item["challenge_z"],
        "status": item["status"],
        "requested_at": item["requested_at"],
        "expires_at": item["expires_at"],
        "resolved_at": item.get("resolved_at"),
        "matched_transaction_id": item.get("matched_transaction_id"),
        "failure_reason": item.get("failure_reason"),
    }
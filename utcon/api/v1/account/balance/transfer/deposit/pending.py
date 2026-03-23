from __future__ import annotations

from fastapi import APIRouter, Query

from utcon import db
from utcon.repositories import deposit as deposit_repo

router = APIRouter(
    prefix="/v1/admin/account/balance/transfer/deposit",
    tags=["admin"],
)


@router.get("/pending")
async def pending_deposits(limit: int = Query(default=100, ge=1, le=500)):
    async with db.connection() as conn:
        await deposit_repo.ensure_deposit_schema(conn)
        await deposit_repo.expire_stale_deposit_challenges(conn)
        rows = await deposit_repo.list_pending_deposit_queue(conn, limit=limit)

    return [_serialize_queue_item(row) for row in rows]


def _serialize_queue_item(item):
    return {
        "id": item["id"],
        "discord_uuid": item["discord_uuid"],
        "challenge_shop_id": item["challenge_shop_id"],
        "challenge_owner_uuid": str(item["challenge_owner_uuid"]),
        "challenge_owner_name": item["challenge_owner_name"],
        "challenge_item_type": item["challenge_item_type"],
        "challenge_item_name": item.get("challenge_item_name"),
        "challenge_item_quantity": int(item["challenge_item_quantity"]),
        "challenge_price": float(item["challenge_price"]),
        "expected_total": float(item["expected_total"]),
        "challenge_world": item["challenge_world"],
        "challenge_x": int(item["challenge_x"]),
        "challenge_y": int(item["challenge_y"]),
        "challenge_z": int(item["challenge_z"]),
        "status": item["status"],
        "requested_at": item["requested_at"].isoformat() if item.get("requested_at") else None,
        "expires_at": item["expires_at"].isoformat() if item.get("expires_at") else None,
        "resolved_at": item["resolved_at"].isoformat() if item.get("resolved_at") else None,
        "matched_transaction_id": item.get("matched_transaction_id"),
        "failure_reason": item.get("failure_reason"),
        "processed_by": item.get("processed_by"),
    }
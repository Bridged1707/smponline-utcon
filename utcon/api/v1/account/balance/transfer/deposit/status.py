from __future__ import annotations

from fastapi import APIRouter, Query

from utcon import db
from utcon.repositories import deposit as deposit_repo

router = APIRouter(prefix="/v1/account/balance/transfer/deposit", tags=["balance"])


@router.get("/status")
async def deposit_status(discord_uuid: str = Query(...)):
    async with db.connection() as conn:
        await deposit_repo.expire_stale_deposit_challenges(conn)
        item = await deposit_repo.get_pending_deposit_for_discord(conn, discord_uuid)

        if item is None:
            return {
                "status": "not_found",
                "discord_uuid": discord_uuid,
            }

    return {
        "status": item["status"],
        "discord_uuid": discord_uuid,
        "challenge": _serialize_queue_item(item),
    }


def _serialize_queue_item(item):
    return {
        "queue_id": item["id"],
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
        "requested_at": item["requested_at"],
        "expires_at": item["expires_at"],
        "resolved_at": item.get("resolved_at"),
        "matched_transaction_id": item.get("matched_transaction_id"),
        "failure_reason": item.get("failure_reason"),
    }
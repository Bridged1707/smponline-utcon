from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import deposit as deposit_repo
from utcon.schemas.deposit import DepositChallengeCreateRequest

router = APIRouter(prefix="/v1/account/balance/transfer/deposit", tags=["balance"])
log = logging.getLogger(__name__)


@router.post("/request")
async def request_deposit(req: DepositChallengeCreateRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            await deposit_repo.ensure_deposit_schema(conn)
            await deposit_repo.expire_stale_deposit_challenges(conn)

            try:
                queue_item = await deposit_repo.create_deposit_challenge(conn, req.discord_uuid)
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                detail = str(exc)
                if detail == "no_deposit_shops_available":
                    raise HTTPException(status_code=503, detail=detail) from exc
                raise HTTPException(status_code=409, detail=detail) from exc
            except Exception as exc:
                log.exception("Deposit challenge creation failed discord_uuid=%s", req.discord_uuid)
                raise HTTPException(status_code=500, detail="deposit_request_failed") from exc

    return {
        "status": queue_item["status"],
        "challenge": _serialize_queue_item(queue_item),
    }


@router.post("")
async def legacy_deposit_route():
    raise HTTPException(
        status_code=410,
        detail="direct deposits are gone; use /v1/account/balance/transfer/deposit/request and complete the shop deposit challenge",
    )


def _serialize_queue_item(item):
    return {
        "queue_id": item["id"],
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
        "requested_at": item["requested_at"],
        "expires_at": item["expires_at"],
        "resolved_at": item.get("resolved_at"),
        "matched_transaction_id": item.get("matched_transaction_id"),
        "failure_reason": item.get("failure_reason"),
        "status": item["status"],
    }
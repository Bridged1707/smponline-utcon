from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import balance as balance_repo
from utcon.repositories import deposit as deposit_repo
from utcon.schemas.deposit import DepositChallengeResolveRequest

router = APIRouter(prefix="/v1/account/balance/transfer/deposit", tags=["balance"])
log = logging.getLogger(__name__)


@router.post("/resolve")
async def resolve_deposit(req: DepositChallengeResolveRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                queue_item = await deposit_repo.resolve_deposit_match(
                    conn,
                    queue_id=req.queue_id,
                    matched_transaction_id=req.matched_transaction_id,
                    processed_by=req.processed_by or "utmp",
                )
            except LookupError as exc:
                detail = str(exc)
                if detail in {"deposit_queue_item_not_found", "transaction_not_found"}:
                    raise HTTPException(status_code=404, detail=detail) from exc
                raise HTTPException(status_code=404, detail="deposit_not_found") from exc
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except Exception as exc:
                log.exception(
                    "Deposit resolve failed queue_id=%s transaction_id=%s",
                    req.queue_id,
                    req.matched_transaction_id,
                )
                raise HTTPException(status_code=500, detail="deposit_resolve_failed") from exc

            balance = await balance_repo.get_balance(conn, queue_item["discord_uuid"])

    return {
        "status": queue_item["status"],
        "queue": _serialize_queue_item(queue_item),
        "balance": float(balance) if balance is not None else None,
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
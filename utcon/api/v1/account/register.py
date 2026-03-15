from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from utcon import db
from utcon.repositories import account as account_repo

router = APIRouter(prefix="/v1/account/register", tags=["account"])


class RegisterRequestPayload(BaseModel):
    discord_uuid: str


class ResolveRegistrationPayload(BaseModel):
    queue_id: int
    matched_shop_id: int
    matched_owner_uuid: str
    matched_owner_name: str


class FailRegistrationPayload(BaseModel):
    queue_id: int
    status: str
    failure_reason: Optional[str] = None


@router.post("/request")
async def request_registration(req: RegisterRequestPayload):
    async with db.connection() as conn:
        async with conn.transaction():
            await account_repo.expire_stale_registrations(conn)
            try:
                queue_item = await account_repo.create_registration_challenge(conn, req.discord_uuid)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc

    return {
        "status": queue_item["status"],
        "challenge": _serialize_queue_item(queue_item),
    }


@router.get("/status")
async def registration_status(discord_uuid: str = Query(...)):
    async with db.connection() as conn:
        await account_repo.expire_stale_registrations(conn)

        account = await account_repo.get_account_by_discord_uuid(conn, discord_uuid)
        if account and account.get("mc_uuid"):
            return {
                "status": "matched",
                "discord_uuid": discord_uuid,
                "mc_uuid": account.get("mc_uuid"),
                "mc_name": account.get("mc_name"),
                "verified_at": account.get("verified_at"),
            }

        queue_item = await account_repo.get_pending_registration_for_discord(conn, discord_uuid)
        if queue_item is not None:
            return {
                "status": queue_item["status"],
                "discord_uuid": discord_uuid,
                "challenge": _serialize_queue_item(queue_item),
            }

        return {
            "status": "not_found",
            "discord_uuid": discord_uuid,
        }


@router.get("/queue")
async def registration_queue(
    status: str = Query(default="pending"),
    limit: int = Query(default=100, ge=1, le=500),
):
    if status != "pending":
        raise HTTPException(status_code=400, detail="only status=pending is supported")

    async with db.connection() as conn:
        await account_repo.expire_stale_registrations(conn)
        items = await account_repo.list_pending_registration_queue(conn, limit=limit)

    return {
        "status": status,
        "count": len(items),
        "items": [_serialize_queue_item(item) for item in items],
    }


@router.post("/resolve")
async def resolve_registration(req: ResolveRegistrationPayload):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                queue_item = await account_repo.resolve_registration_match(
                    conn,
                    queue_id=req.queue_id,
                    matched_shop_id=req.matched_shop_id,
                    matched_owner_uuid=req.matched_owner_uuid,
                    matched_owner_name=req.matched_owner_name,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc

            account = await account_repo.get_account_by_discord_uuid(conn, queue_item["discord_uuid"])

    return {
        "status": queue_item["status"],
        "queue": _serialize_queue_item(queue_item),
        "account": {
            "discord_uuid": account.get("discord_uuid") if account else None,
            "mc_uuid": account.get("mc_uuid") if account else None,
            "mc_name": account.get("mc_name") if account else None,
            "verified_at": account.get("verified_at") if account else None,
        },
    }


@router.post("/fail")
async def fail_registration(req: FailRegistrationPayload):
    if req.status not in {
        account_repo.REGISTER_STATUS_FAILED,
        account_repo.REGISTER_STATUS_EXPIRED,
        account_repo.REGISTER_STATUS_CANCELLED,
    }:
        raise HTTPException(status_code=400, detail="invalid failure status")

    async with db.connection() as conn:
        async with conn.transaction():
            queue_item = await account_repo.mark_registration_status(
                conn,
                queue_id=req.queue_id,
                status=req.status,
                failure_reason=req.failure_reason,
            )

    if queue_item is None:
        raise HTTPException(status_code=404, detail="registration queue item not found")

    return {
        "status": queue_item["status"],
        "queue": _serialize_queue_item(queue_item),
    }


# Backward-compatible placeholder to make old callers fail clearly instead of silently doing the wrong thing.
@router.post("")
async def legacy_register_route():
    raise HTTPException(
        status_code=410,
        detail="direct account registration is gone; use /v1/account/register/request and complete the shop verification flow",
    )


def _serialize_queue_item(item):
    return {
        "queue_id": item["id"],
        "discord_uuid": item["discord_uuid"],
        "item_type": item["challenge_item_type"],
        "item_name": item.get("challenge_item_name"),
        "price": float(item["challenge_price"]),
        "item_quantity": int(item["challenge_item_quantity"]),
        "shop_type": item["challenge_shop_type"],
        "requested_at": item["requested_at"],
        "expires_at": item["expires_at"],
        "status": item["status"],
        "matched_shop_id": item.get("matched_shop_id"),
        "matched_owner_uuid": str(item["matched_owner_uuid"]) if item.get("matched_owner_uuid") else None,
        "matched_owner_name": item.get("matched_owner_name"),
        "resolved_at": item.get("resolved_at"),
        "failure_reason": item.get("failure_reason"),
        "attempt_count": item.get("attempt_count", 0),
    }

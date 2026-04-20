from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import balance_notifications as balance_notifications_repo
from utcon.schemas.balance_notifications import (
    BalanceNotificationCreateRequest,
    BalanceNotificationDeliveryResultRequest,
)

router = APIRouter(tags=["balance-notifications"])


@router.post("/api/v1/notifications/balance/create")
async def create_balance_notification(req: BalanceNotificationCreateRequest):
    amount = Decimal(str(req.amount))
    if amount <= 0:
        raise HTTPException(status_code=400, detail="invalid_amount")

    async with db.connection() as conn:
        async with conn.transaction():
            item = await balance_notifications_repo.create_balance_notification(
                conn,
                discord_uuid=req.discord_uuid,
                amount=amount,
                reason=req.reason,
                source=req.source,
                metadata=req.metadata,
            )

    return _serialize_notification(item)


@router.get("/api/v1/notifications/balance/pending")
async def list_pending_balance_notifications(
    limit: int = Query(default=100, ge=1, le=500),
):
    async with db.connection() as conn:
        items = await balance_notifications_repo.list_pending_balance_notifications(
            conn,
            limit=limit,
        )

    return {
        "items": [_serialize_notification(item) for item in items],
        "count": len(items),
    }


@router.post("/api/v1/notifications/balance/{notification_id}/sent")
async def mark_balance_notification_sent(notification_id: int):
    async with db.connection() as conn:
        async with conn.transaction():
            item = await balance_notifications_repo.mark_balance_notification_sent(
                conn,
                notification_id,
            )
            if item is None:
                raise HTTPException(status_code=404, detail="balance_notification_not_found")

    return _serialize_notification(item)


@router.post("/api/v1/notifications/balance/{notification_id}/failed")
async def mark_balance_notification_failed(
    notification_id: int,
    req: BalanceNotificationDeliveryResultRequest,
):
    async with db.connection() as conn:
        async with conn.transaction():
            item = await balance_notifications_repo.mark_balance_notification_failed(
                conn,
                notification_id,
                req.error or "",
            )
            if item is None:
                raise HTTPException(status_code=404, detail="balance_notification_not_found")

    return _serialize_notification(item)


def _serialize_notification(item):
    return {
        "id": item["id"],
        "discord_uuid": item["discord_uuid"],
        "amount": float(item["amount"]),
        "reason": item.get("reason"),
        "source": item.get("source"),
        "metadata": item.get("metadata") or {},
        "status": item["status"],
        "created_at": item["created_at"],
        "sent_at": item.get("sent_at"),
        "last_error": item.get("last_error"),
    }
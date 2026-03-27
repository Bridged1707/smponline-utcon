from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import predictions as prediction_repo
from utcon.schemas.predictions import PredictionNotificationFailureRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("/settlements/pending")
async def list_pending_prediction_settlements(
    limit: int = Query(default=100, ge=1, le=500),
):
    async with db.connection() as conn:
        items = await prediction_repo.list_pending_settlement_notifications(conn, limit=limit)
    return {"items": items, "count": len(items)}


@router.post("/wagers/{wager_id}/notified")
async def mark_prediction_wager_notified(wager_id: int):
    async with db.connection() as conn:
        async with conn.transaction():
            item = await prediction_repo.mark_wager_notification_delivered(conn, wager_id)
            if item is None:
                raise HTTPException(status_code=404, detail="prediction_wager_not_found")
    return {"status": "ok", "item": item}


@router.post("/wagers/{wager_id}/notification-failed")
async def mark_prediction_wager_notification_failed(wager_id: int, req: PredictionNotificationFailureRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            item = await prediction_repo.mark_wager_notification_failed(conn, wager_id, req.error or "")
            if item is None:
                raise HTTPException(status_code=404, detail="prediction_wager_not_found")
    return {"status": "ok", "item": item}

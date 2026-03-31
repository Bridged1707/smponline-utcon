from __future__ import annotations

from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from utcon import db
from utcon.repositories import predictions as repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


class PredictionSettlementProcessRequest(BaseModel):
    market_code: Optional[str] = None
    limit: int = Field(default=500, ge=1, le=5000)
    processed_by: Optional[str] = None


class PredictionNotificationFailureRequest(BaseModel):
    error: Optional[str] = Field(default=None, max_length=1000)


@router.get("/settlements/pending")
async def list_pending_prediction_settlements(limit: int = 100):
    async with db.connection() as conn:
        items = await repo.list_pending_settlements(conn, limit=limit)
    return {"items": items}


@router.post("/settlements/process")
async def process_pending_prediction_settlements(req: PredictionSettlementProcessRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            result = await repo.process_pending_settlements(
                conn,
                market_code=req.market_code.strip().upper() if req.market_code else None,
                limit=req.limit,
                processed_by=req.processed_by,
            )
    return {"status": "ok", **result}


@router.get("/notifications/pending")
async def list_pending_prediction_notifications(limit: int = 100):
    async with db.connection() as conn:
        items = await repo.list_pending_notifications(conn, limit=limit)
    return {"items": items}


@router.post("/notifications/{wager_id}/sent")
async def mark_prediction_notification_sent(wager_id: int):
    async with db.connection() as conn:
        async with conn.transaction():
            result = await repo.mark_notification_sent(conn, wager_id)
    return result


@router.post("/notifications/{wager_id}/failed")
async def mark_prediction_notification_failed(wager_id: int, req: PredictionNotificationFailureRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            result = await repo.mark_notification_failed(conn, wager_id, error=req.error)
    return result

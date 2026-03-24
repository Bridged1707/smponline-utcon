from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import alerts as alert_repo
from utcon.schemas.alerts import (
    AlertEventCreateRequest,
    AlertEventDeliveryResultRequest,
)

router = APIRouter(tags=["alerts"])


@router.post("/api/v1/alerts/events/create")
async def create_alert_event(req: AlertEventCreateRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            event = await alert_repo.create_event(conn, req.dict())
    return event


@router.get("/api/v1/alerts/events/pending")
async def list_pending_alert_events(
    limit: int = Query(default=100, ge=1, le=500),
):
    async with db.connection() as conn:
        items = await alert_repo.list_pending_events(conn, limit=limit)
    return {"items": items, "count": len(items)}


@router.post("/api/v1/alerts/events/{event_id}/delivered")
async def mark_alert_event_delivered(event_id: int):
    async with db.connection() as conn:
        async with conn.transaction():
            event = await alert_repo.mark_event_delivered(conn, event_id)
            if event is None:
                raise HTTPException(status_code=404, detail="alert_event_not_found")
    return event


@router.post("/api/v1/alerts/events/{event_id}/failed")
async def mark_alert_event_failed(event_id: int, req: AlertEventDeliveryResultRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            event = await alert_repo.mark_event_failed(conn, event_id, req.error or "")
            if event is None:
                raise HTTPException(status_code=404, detail="alert_event_not_found")
    return event
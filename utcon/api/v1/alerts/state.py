from __future__ import annotations

from fastapi import APIRouter

from utcon import db
from utcon.repositories import alerts as alert_repo
from utcon.schemas.alerts import AlertStateUpsertRequest

router = APIRouter(prefix="/api/v1/alerts/state", tags=["alerts-internal"])


@router.get("/{alert_id}")
async def list_alert_states(alert_id: int):
    async with db.connection() as conn:
        items = await alert_repo.list_states_for_alert(conn, alert_id)
    return {"items": items, "count": len(items)}


@router.get("/{alert_id}/{state_key}")
async def get_alert_state(alert_id: int, state_key: str):
    async with db.connection() as conn:
        item = await alert_repo.get_state(conn, alert_id, state_key)
    return {"state": item}


@router.post("/upsert")
async def upsert_alert_state(req: AlertStateUpsertRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            item = await alert_repo.upsert_state(conn, req.dict())
    return {"status": "ok", "state": item}
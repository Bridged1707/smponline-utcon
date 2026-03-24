from __future__ import annotations

from fastapi import APIRouter

from utcon import db
from utcon.repositories import alerts as alert_repo
from utcon.schemas.alerts import AlertStateUpsertRequest

router = APIRouter(tags=["alerts-internal"])


@router.get("/api/v1/alerts/state/{alert_id}")
async def list_alert_states_legacy(alert_id: int):
    async with db.connection() as conn:
        items = await alert_repo.list_states_for_alert(conn, alert_id)
    return {"items": items, "count": len(items)}


@router.get("/api/v1/alerts/state/{alert_id}/{state_key}")
async def get_alert_state_legacy(alert_id: int, state_key: str):
    async with db.connection() as conn:
        item = await alert_repo.get_state(conn, alert_id, state_key)
    return {"state": item}


@router.post("/api/v1/alerts/state/upsert")
async def upsert_alert_state_legacy(req: AlertStateUpsertRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            item = await alert_repo.upsert_state(conn, req.dict())
    return {"status": "ok", "state": item}


# Compatibility routes for UTMP client
@router.get("/api/v1/alerts/{alert_id}/states/{state_key}")
async def get_alert_state(alert_id: int, state_key: str):
    async with db.connection() as conn:
        item = await alert_repo.get_state(conn, alert_id, state_key)
    return item or {}


@router.get("/api/v1/alerts/{alert_id}/states")
async def list_alert_states(alert_id: int):
    async with db.connection() as conn:
        items = await alert_repo.list_states_for_alert(conn, alert_id)
    return {"items": items, "count": len(items)}


@router.post("/api/v1/alerts/states/upsert")
async def upsert_alert_state(req: AlertStateUpsertRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            item = await alert_repo.upsert_state(conn, req.dict())
    return item
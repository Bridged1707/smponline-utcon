from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import alerts as alert_repo
from utcon.schemas.alerts import AlertCreateRequest

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.post("/create")
async def create_alert(req: AlertCreateRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                alert = await alert_repo.create_alert(conn, req.dict())
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok", "alert": alert}
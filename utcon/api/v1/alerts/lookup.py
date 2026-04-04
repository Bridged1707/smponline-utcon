from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import alerts as alert_repo

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("/{alert_id}")
async def lookup_alert(alert_id: int):
    async with db.connection() as conn:
        alert = await alert_repo.get_alert(conn, alert_id)
        if alert is None:
            raise HTTPException(status_code=404, detail="alert_not_found")
    return {"alert": alert}
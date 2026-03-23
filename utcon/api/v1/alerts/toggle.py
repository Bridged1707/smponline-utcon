from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import alerts as alert_repo
from utcon.schemas.alerts import AlertToggleRequest

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.post("/{alert_id}/toggle")
async def toggle_alert(alert_id: int, req: AlertToggleRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            if not await alert_repo.get_alert(conn, alert_id):
                raise HTTPException(status_code=404, detail="alert_not_found")
            if not await alert_repo.alert_belongs_to_discord_uuid(conn, alert_id, req.discord_uuid):
                raise HTTPException(status_code=403, detail="You can only toggle your own alerts.")
            alert = await alert_repo.set_alert_active(conn, alert_id, req.discord_uuid, req.is_active)
            if alert is None:
                raise HTTPException(status_code=404, detail="alert_not_found")
    return {"status": "ok", "alert": alert}
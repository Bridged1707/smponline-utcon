from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import alerts as alert_repo
from utcon.schemas.alerts import AlertUpdateRequest

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.patch("/{alert_id}")
async def update_alert(alert_id: int, req: AlertUpdateRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            if not await alert_repo.get_alert(conn, alert_id):
                raise HTTPException(status_code=404, detail="alert_not_found")
            if not await alert_repo.alert_belongs_to_discord_uuid(conn, alert_id, req.discord_uuid):
                raise HTTPException(status_code=403, detail="You can only edit your own alerts.")
            try:
                alert = await alert_repo.update_alert(conn, alert_id, req.dict())
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            if alert is None:
                raise HTTPException(status_code=404, detail="alert_not_found")
    return {"status": "ok", "alert": alert}

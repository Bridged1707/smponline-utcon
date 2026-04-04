from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import alerts as alert_repo

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.delete("/{alert_id}")
async def delete_alert(alert_id: int, discord_uuid: str = Query(..., min_length=1, max_length=64)):
    async with db.connection() as conn:
        async with conn.transaction():
            if not await alert_repo.get_alert(conn, alert_id):
                raise HTTPException(status_code=404, detail="alert_not_found")
            if not await alert_repo.alert_belongs_to_discord_uuid(conn, alert_id, discord_uuid):
                raise HTTPException(status_code=403, detail="You can only remove your own alerts.")
            deleted = await alert_repo.delete_alert(conn, alert_id, discord_uuid)
            if not deleted:
                raise HTTPException(status_code=404, detail="alert_not_found")
    return {"status": "ok", "deleted": True}

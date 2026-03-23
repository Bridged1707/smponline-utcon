from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import alerts as alert_repo

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.delete("/{alert_id}")
async def delete_alert(alert_id: int):
    async with db.connection() as conn:
        async with conn.transaction():
            deleted = await alert_repo.delete_alert(conn, alert_id)
            if not deleted:
                raise HTTPException(status_code=404, detail="alert_not_found")
    return {"status": "ok", "deleted": True}
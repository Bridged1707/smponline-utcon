from __future__ import annotations

from fastapi import APIRouter, Query

from utcon import db
from utcon.repositories import alerts as alert_repo

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("")
async def list_alerts(
    discord_uuid: str | None = None,
    active_only: bool = False,
    limit: int = Query(default=200, ge=1, le=1000),
):
    async with db.connection() as conn:
        items = await alert_repo.list_alerts(
            conn,
            discord_uuid=discord_uuid,
            active_only=active_only,
            limit=limit,
        )
    return {"items": items, "count": len(items)}
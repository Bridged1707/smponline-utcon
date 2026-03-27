from __future__ import annotations

from fastapi import APIRouter, Query

from utcon import db
from utcon.repositories import predictions as prediction_repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("/wagers")
async def list_prediction_wagers(
    discord_uuid: str,
    unsettled_only: bool = Query(default=True),
    limit: int = Query(default=25, ge=1, le=100),
):
    async with db.connection() as conn:
        items = await prediction_repo.list_user_wagers(
            conn,
            discord_uuid=discord_uuid,
            unsettled_only=unsettled_only,
            limit=limit,
        )
    return {"items": items, "count": len(items)}

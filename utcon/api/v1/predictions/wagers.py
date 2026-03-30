from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("/wagers/{discord_uuid}")
async def list_prediction_wagers(
    discord_uuid: str,
    unsettled_only: bool = True,
    limit: int = 25,
):
    async with db.connection() as conn:
        try:
            items = await repo.list_user_wagers(
                conn,
                discord_uuid=discord_uuid,
                unsettled_only=unsettled_only,
                limit=limit,
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {"items": items}
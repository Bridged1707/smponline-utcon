from __future__ import annotations

from fastapi import APIRouter, Query

from utcon import db
from utcon.repositories import predictions as prediction_repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("")
async def list_predictions(
    status: str | None = None,
    include_closed: bool = False,
    limit: int = Query(default=25, ge=1, le=250),
):
    async with db.connection() as conn:
        items = await prediction_repo.list_markets(
            conn,
            status=status,
            include_closed=include_closed,
            limit=limit,
        )
    return {"items": items, "count": len(items)}

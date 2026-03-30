from __future__ import annotations

from typing import Optional

from fastapi import APIRouter

from utcon import db
from utcon.repositories import predictions as repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("")
async def list_predictions(
    status: Optional[str] = None,
    include_closed: bool = False,
    limit: int = 25,
):
    async with db.connection() as conn:
        items = await repo.list_markets(
            conn,
            status=status,
            include_closed=include_closed,
            limit=limit,
        )
    return {"items": items}
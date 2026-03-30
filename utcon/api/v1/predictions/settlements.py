from __future__ import annotations

from fastapi import APIRouter

from utcon import db
from utcon.repositories import predictions as repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("/settlements/pending")
async def list_pending_prediction_settlements(limit: int = 100):
    async with db.connection() as conn:
        items = await repo.list_pending_settlements(conn, limit=limit)
    return {"items": items}
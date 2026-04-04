from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as repo
from utcon.schemas.predictions import PredictionMarketCreateRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.post("/create")
async def create_prediction_market(req: PredictionMarketCreateRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await repo.create_market(conn, req)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"status": "ok", **payload}
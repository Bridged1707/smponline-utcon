from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as prediction_repo
from utcon.schemas.predictions import PredictionMarketCreateRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.post("/create")
async def create_prediction(req: PredictionMarketCreateRequest):
    if req.resolves_at is not None and req.resolves_at < req.closes_at:
        raise HTTPException(status_code=400, detail="resolves_at_must_be_after_closes_at")

    async with db.connection() as conn:
        async with conn.transaction():
            existing = await prediction_repo.get_market(conn, req.code.strip().upper())
            if existing is not None:
                raise HTTPException(status_code=409, detail="market_already_exists")
            market = await prediction_repo.create_market(conn, req.model_dump())
    return {"status": "ok", "market": market}

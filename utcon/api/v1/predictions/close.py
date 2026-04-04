from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as repo
from utcon.schemas.predictions import PredictionCloseRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


async def _close_prediction_market(req: PredictionCloseRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await repo.close_market(
                    conn,
                    market_code=req.market_code.strip().upper(),
                    closed_by=req.closed_by,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"status": "ok", **payload}


@router.post("/close")
async def close_prediction_market(req: PredictionCloseRequest):
    return await _close_prediction_market(req)


@router.post("/{market_code}/close")
async def close_prediction_market_by_path(market_code: str, req: PredictionCloseRequest | None = None):
    return await _close_prediction_market(
        PredictionCloseRequest(
            market_code=market_code.strip().upper(),
            closed_by=req.closed_by if req else None,
        )
    )

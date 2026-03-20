from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as prediction_repo
from utcon.schemas.predictions import PredictionResolveRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.post("/{market_code}/resolve")
async def resolve_prediction(market_code: str, req: PredictionResolveRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await prediction_repo.resolve_market(
                    conn,
                    market_code=market_code.strip().upper(),
                    outcome=req.outcome,
                    resolved_by=req.resolved_by,
                    resolution_notes=req.resolution_notes,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok", **payload}

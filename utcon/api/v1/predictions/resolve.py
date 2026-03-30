from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as repo
from utcon.schemas.predictions import PredictionResolveRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.post("/resolve")
async def resolve_prediction_market(req: PredictionResolveRequest):
    if not req.option_code:
        raise HTTPException(status_code=400, detail="option_code_required")

    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await repo.resolve_market(
                    conn,
                    market_code=req.market_code.strip().upper(),
                    option_code=req.option_code.strip().upper(),
                    resolved_by=req.resolved_by,
                    resolution_notes=req.resolution_notes,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"status": "ok", **payload}
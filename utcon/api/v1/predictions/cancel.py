from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as repo
from utcon.schemas.predictions import PredictionCancelRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.post("/cancel")
async def cancel_prediction_market(req: PredictionCancelRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await repo.cancel_market(
                    conn,
                    market_code=req.market_code.strip().upper(),
                    cancelled_by=req.cancelled_by,
                    reason=req.reason,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"status": "ok", **payload}
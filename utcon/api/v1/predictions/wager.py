from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as repo
from utcon.schemas.predictions import PredictionWagerRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.post("/wager")
async def place_prediction_wager(req: PredictionWagerRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await repo.place_wager(conn, req)
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"status": "ok", **payload}
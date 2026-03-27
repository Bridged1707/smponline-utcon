from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as prediction_repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.post("/{market_code}/close")
async def close_prediction(market_code: str, closed_by: str | None = None, close_reason: str | None = None):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await prediction_repo.close_market(
                    conn,
                    market_code=market_code.strip().upper(),
                    closed_by=closed_by,
                    close_reason=close_reason,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok", **payload}

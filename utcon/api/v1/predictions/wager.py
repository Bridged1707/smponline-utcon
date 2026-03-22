from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as prediction_repo
from utcon.schemas.predictions import PredictionWagerRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])
log = logging.getLogger(__name__)


@router.post("/wager")
async def place_prediction_wager(req: PredictionWagerRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await prediction_repo.place_wager(
                    conn,
                    market_code=req.market_code.strip().upper(),
                    discord_uuid=req.discord_uuid,
                    side=req.side,
                    amount=req.amount,
                )
            except LookupError as exc:
                detail = str(exc)
                if detail in {"market_not_found", "account_not_found", "balance_not_found"}:
                    raise HTTPException(status_code=404, detail=detail) from exc
                raise HTTPException(status_code=404, detail="resource_not_found") from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:
                log.exception(
                    "Prediction wager failed for market_code=%s discord_uuid=%s side=%s amount=%s",
                    req.market_code,
                    req.discord_uuid,
                    req.side,
                    req.amount,
                )
                raise HTTPException(status_code=500, detail="prediction_wager_failed") from exc
    return {"status": "ok", **payload}
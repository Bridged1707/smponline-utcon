from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as prediction_repo
from utcon.schemas.predictions import PredictionWagerRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


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
    return {"status": "ok", **payload}

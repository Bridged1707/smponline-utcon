from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as prediction_repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("/{market_code}/options")
async def list_prediction_options(market_code: str):
    normalized_market_code = market_code.strip().upper()

    async with db.connection() as conn:
        market = await prediction_repo.get_market(conn, normalized_market_code)
        if market is None:
            raise HTTPException(status_code=404, detail="market_not_found")

        options = await prediction_repo.get_market_options(conn, normalized_market_code)

    return {
        "market": market,
        "options": options,
        "count": len(options),
    }
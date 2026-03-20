from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import predictions as prediction_repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("/{market_code}")
async def lookup_prediction(market_code: str, include_recent_wagers: bool = Query(default=True)):
    async with db.connection() as conn:
        market = await prediction_repo.get_market(conn, market_code.strip().upper())
        if market is None:
            raise HTTPException(status_code=404, detail="market_not_found")
        recent_wagers = []
        if include_recent_wagers:
            recent_wagers = await prediction_repo.get_recent_wagers(conn, market_code.strip().upper(), limit=20)
    return {
        "market": market,
        "recent_wagers": recent_wagers,
    }

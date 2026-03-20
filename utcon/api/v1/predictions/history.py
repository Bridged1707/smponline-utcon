from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import predictions as prediction_repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("/{market_code}/history")
async def prediction_history(
    market_code: str,
    from_ts: int = Query(default=0, ge=0),
    to_ts: int = Query(default=9_999_999_999_999, ge=0),
    limit: int = Query(default=500, ge=1, le=5000),
    newest_first: bool = False,
):
    async with db.connection() as conn:
        market = await prediction_repo.get_market(conn, market_code.strip().upper())
        if market is None:
            raise HTTPException(status_code=404, detail="market_not_found")
        items = await prediction_repo.get_history(
            conn,
            market_code.strip().upper(),
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            newest_first=newest_first,
        )
    return {"market": market, "items": items, "count": len(items)}

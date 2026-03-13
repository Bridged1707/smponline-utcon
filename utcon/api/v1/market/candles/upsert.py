from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import market_data as market_repo
from utcon.schemas.market import MarketCandlesUpsertRequest

router = APIRouter(prefix="/api/v1/market/candles", tags=["market"])


@router.post("/upsert")
async def upsert_market_candles(payload: MarketCandlesUpsertRequest):
    async with db.connection() as conn:
        symbol = await market_repo.get_market_symbol(conn, payload.symbol_code)
        if not symbol:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        count = await market_repo.upsert_market_candles(
            conn,
            payload.symbol_code,
            payload.interval_key,
            [c.dict() for c in payload.candles],
        )

    return {
        "status": "ok",
        "symbol_code": payload.symbol_code,
        "interval_key": payload.interval_key,
        "count": count,
    }
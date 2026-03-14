from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import market_data as market_repo
from utcon.schemas.market import MarketQuoteSampleUpsertRequest

router = APIRouter(prefix="/api/v1/market/samples", tags=["market"])


@router.post("/upsert")
async def upsert_market_quote_sample(payload: MarketQuoteSampleUpsertRequest):
    async with db.connection() as conn:
        symbol = await market_repo.get_market_symbol(conn, payload.symbol_code)
        if not symbol:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        await market_repo.upsert_market_quote_sample(conn, payload.dict())

    return {
        "status": "ok",
        "symbol_code": payload.symbol_code,
        "sample_ts": payload.sample_ts,
    }
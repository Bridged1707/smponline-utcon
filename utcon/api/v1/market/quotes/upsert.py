from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import market_data as market_repo
from utcon.schemas.market import MarketQuoteUpsertRequest

router = APIRouter(prefix="/api/v1/market/quotes", tags=["market"])


@router.post("/upsert")
async def upsert_market_quote(payload: MarketQuoteUpsertRequest):
    async with db.connection() as conn:
        symbol = await market_repo.get_market_symbol(conn, payload.symbol_code)
        if not symbol:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        await market_repo.upsert_market_quote(conn, payload.dict())

    return {
        "status": "ok",
        "symbol_code": payload.symbol_code,
    }
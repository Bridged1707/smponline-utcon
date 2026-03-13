from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import market_data as market_repo

router = APIRouter(prefix="/api/v1/market/quote", tags=["market"])


@router.get("/{symbol_code}")
async def lookup_market_quote(symbol_code: str):
    async with db.connection() as conn:
        quote = await market_repo.get_market_quote(conn, symbol_code)
        if not quote:
            raise HTTPException(status_code=404, detail="quote_not_found")

    return quote
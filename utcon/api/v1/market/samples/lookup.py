from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import market_data as market_repo

router = APIRouter(prefix="/api/v1/market/samples", tags=["market"])


@router.get("/{symbol_code}")
async def lookup_market_quote_samples(
    symbol_code: str,
    from_ts: int = Query(..., ge=0),
    to_ts: int = Query(..., ge=0),
    limit: Optional[int] = Query(default=None, ge=1, le=10000),
    newest_first: bool = False,
):
    async with db.connection() as conn:
        symbol = await market_repo.get_market_symbol(conn, symbol_code)
        if not symbol:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        items = await market_repo.get_market_quote_samples(
            conn,
            symbol_code=symbol_code,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            newest_first=newest_first,
        )

    return {
        "symbol_code": symbol_code,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "count": len(items),
        "items": items,
    }
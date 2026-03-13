from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import market_config as market_repo

router = APIRouter(prefix="/api/v1/market/symbols", tags=["market"])


@router.get("/{symbol_code}")
async def lookup_market_symbol(symbol_code: str):
    async with db.connection() as conn:
        symbol = await market_repo.get_symbol(conn, symbol_code)
        if not symbol:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        families = await market_repo.get_symbol_families(conn, symbol_code)
        form_rules = await market_repo.get_symbol_form_rules(conn, symbol_code)
        item_overrides = await market_repo.get_symbol_item_overrides(conn, symbol_code)

    return {
        "symbol": symbol,
        "families": families,
        "form_rules": form_rules,
        "item_overrides": item_overrides,
    }
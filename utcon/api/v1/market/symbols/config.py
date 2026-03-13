from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import market_config as market_repo
from utcon.repositories import market_runtime_config as runtime_repo

router = APIRouter(prefix="/api/v1/market/symbols", tags=["market"])


@router.get("/{symbol_code}/config")
async def lookup_symbol_runtime_config(symbol_code: str):
    async with db.connection() as conn:
        symbol = await market_repo.get_symbol(conn, symbol_code)
        if not symbol:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        config = await runtime_repo.get_symbol_config(conn, symbol_code)

    if not config:
        return {
            "symbol_code": symbol_code,
            "is_enabled": True,
            "quote_strategy": None,
            "candle_strategy": None,
            "transaction_lookback_ms": None,
            "shop_stale_after_ms": None,
            "min_trade_count": None,
            "min_trade_volume": None,
            "outlier_filter_enabled": None,
            "carry_forward_enabled": None,
            "enabled_intervals": None,
            "extra_config": {},
        }

    return config
from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import market_data as market_repo
from utcon.services.market_symbols import build_symbol_composition, resolve_symbol_from_query

router = APIRouter(prefix="/api/v1/market/cap", tags=["market"])

DEFAULT_STALE_AFTER_SECONDS = 900
PRICE_SOURCE_PRIORITY = (
    ("mark_price", "mark_price"),
    ("mid_price", "mid_price"),
    ("last_trade_price", "last_trade_price"),
    ("best_bid", "best_bid"),
    ("best_ask", "best_ask"),
)


def choose_unit_price(quote: dict | None) -> tuple[float | None, str | None]:
    if not quote:
        return None, None
    for key, label in PRICE_SOURCE_PRIORITY:
        value = quote.get(key)
        if value is not None:
            return float(value), label
    return None, None


@router.get("/lookup")
async def lookup_market_cap(
    query: str = Query(..., min_length=1),
    stale_after_seconds: int = Query(default=DEFAULT_STALE_AFTER_SECONDS, ge=0, le=86400),
):
    now_ms = int(time.time() * 1000)
    last_seen_since_ts = None if stale_after_seconds == 0 else now_ms - (stale_after_seconds * 1000)

    async with db.connection() as conn:
        resolved = await resolve_symbol_from_query(conn, query)
        if not resolved:
            raise HTTPException(status_code=404, detail="symbol_or_item_not_found")

        composition = await build_symbol_composition(conn, resolved.symbol["code"])
        if not composition:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        quote = await market_repo.get_market_quote(conn, resolved.symbol["code"])
        volume = await market_repo.get_market_cap_snapshot(
            conn,
            composition["items"],
            last_seen_since_ts=last_seen_since_ts,
        )

    unit_price, unit_price_source = choose_unit_price(quote)
    known_volume = float(volume["known_volume"])
    known_market_cap = (unit_price * known_volume) if unit_price is not None else None

    return {
        "query": query,
        "matched_by": resolved.matched_by,
        "matched_value": resolved.matched_value,
        "symbol": composition["symbol"],
        "quote": quote,
        "price": {
            "unit_price": unit_price,
            "unit_price_source": unit_price_source,
        },
        "metrics": {
            "known_volume": known_volume,
            "raw_units": float(volume["raw_units"]),
            "known_market_cap": known_market_cap,
            "shop_count": int(volume["shop_count"]),
        },
        "freshness": {
            "as_of_ts": now_ms,
            "last_seen_since_ts": last_seen_since_ts,
            "stale_after_seconds": stale_after_seconds,
        },
        "composition": {
            "family_count": len(composition["families"]),
            "item_count": len(composition["items"]),
        },
    }

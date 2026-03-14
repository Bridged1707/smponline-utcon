import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from utcon import db
from utcon.repositories import market_data as market_repo

router = APIRouter(prefix="/api/v1/market/chart", tags=["market"])


RANGE_CONFIG: Dict[str, Dict[str, int | str | None]] = {
    "1M": {"interval": "1m", "duration_ms": 1 * 60 * 1000},
    "15M": {"interval": "1m", "duration_ms": 15 * 60 * 1000},
    "1H": {"interval": "1m", "duration_ms": 1 * 60 * 60 * 1000},
    "3H": {"interval": "5m", "duration_ms": 3 * 60 * 60 * 1000},
    "12H": {"interval": "15m", "duration_ms": 12 * 60 * 60 * 1000},
    "1D": {"interval": "5m", "duration_ms": 1 * 24 * 60 * 60 * 1000},
    "7D": {"interval": "1h", "duration_ms": 7 * 24 * 60 * 60 * 1000},
    "14D": {"interval": "4h", "duration_ms": 14 * 24 * 60 * 60 * 1000},
    "1MO": {"interval": "1d", "duration_ms": 30 * 24 * 60 * 60 * 1000},
    "1W": {"interval": "15m", "duration_ms": 7 * 24 * 60 * 60 * 1000},
    "3M": {"interval": "4h", "duration_ms": 90 * 24 * 60 * 60 * 1000},
    "1Y": {"interval": "1d", "duration_ms": 365 * 24 * 60 * 60 * 1000},
    "ALL": {"interval": "1d", "duration_ms": None},
}

INTERVAL_TO_MS = {
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}


def align_bucket_start(ts_ms: int, interval_ms: int) -> int:
    return (ts_ms // interval_ms) * interval_ms


def compute_chart_stats(candles: List[dict]) -> dict:
    if not candles:
        return {
            "open": None,
            "close": None,
            "high": None,
            "low": None,
            "change": None,
            "change_pct": None,
            "volume": 0.0,
            "trade_count": 0,
        }

    first_open = candles[0].get("open")
    last_close = candles[-1].get("close")

    highs = [c["high"] for c in candles if c.get("high") is not None]
    lows = [c["low"] for c in candles if c.get("low") is not None]

    volume = sum(float(c.get("trade_volume") or 0) for c in candles)
    trade_count = sum(int(c.get("trade_count") or 0) for c in candles)

    change = None
    change_pct = None
    if first_open is not None and last_close is not None:
        change = float(last_close) - float(first_open)
        if float(first_open) != 0:
            change_pct = (change / float(first_open)) * 100.0

    return {
        "open": first_open,
        "close": last_close,
        "high": max(highs) if highs else None,
        "low": min(lows) if lows else None,
        "change": change,
        "change_pct": change_pct,
        "volume": volume,
        "trade_count": trade_count,
    }


def densify_candles(
    candles: List[dict],
    interval_key: str,
    from_ts: int,
    to_ts: int,
    seed_candle: Optional[dict] = None,
) -> List[dict]:
    interval_ms = INTERVAL_TO_MS[interval_key]
    aligned_from = align_bucket_start(from_ts, interval_ms)
    aligned_to = align_bucket_start(to_ts, interval_ms)

    candle_map = {
        int(c["bucket_start_ts"]): dict(c)
        for c in candles
    }

    result: List[dict] = []

    carry_close = seed_candle.get("close") if seed_candle else None
    carry_best_bid = seed_candle.get("best_bid") if seed_candle else None
    carry_best_ask = seed_candle.get("best_ask") if seed_candle else None
    carry_midpoint = seed_candle.get("midpoint") if seed_candle else None

    bucket_start = aligned_from
    while bucket_start <= aligned_to:
        candle = candle_map.get(bucket_start)
        if candle is not None:
            result.append(candle)
            if candle.get("close") is not None:
                carry_close = candle.get("close")
            if candle.get("best_bid") is not None:
                carry_best_bid = candle.get("best_bid")
            if candle.get("best_ask") is not None:
                carry_best_ask = candle.get("best_ask")
            if candle.get("midpoint") is not None:
                carry_midpoint = candle.get("midpoint")
        elif carry_close is not None:
            result.append({
                "symbol_code": candles[0].get("symbol_code") if candles else None,
                "interval_key": interval_key,
                "bucket_start_ts": bucket_start,
                "bucket_end_ts": bucket_start + interval_ms - 1,
                "open": carry_close,
                "high": carry_close,
                "low": carry_close,
                "close": carry_close,
                "vwap": None,
                "median": carry_close,
                "trade_volume": 0.0,
                "trade_count": 0,
                "buy_volume": 0.0,
                "sell_volume": 0.0,
                "best_bid": carry_best_bid,
                "best_ask": carry_best_ask,
                "midpoint": carry_midpoint,
                "source_trade_count": 0,
                "source_shop_count": 0,
                "updated_at": None,
            })

        bucket_start += interval_ms

    return result


@router.get("/{symbol_code}")
async def lookup_market_chart(
    symbol_code: str,
    range_key: str = Query(default="1D", alias="range"),
):
    raw_range = range_key.strip()
    normalized = raw_range.upper()

    aliases = {
        "1MIN": "1M",
        "15MIN": "15M",
        "60M": "1H",
        "1HR": "1H",
        "3HR": "3H",
        "12HR": "12H",
        "1DAY": "1D",
        "7DAY": "7D",
        "14DAY": "14D",
        "1MONTH": "1MO",
    }

    range_key = aliases.get(normalized, normalized)

    if range_key not in RANGE_CONFIG:
        raise HTTPException(status_code=400, detail="invalid_range")

    config = RANGE_CONFIG[range_key]
    interval_key = config["interval"]
    now_ms = int(time.time() * 1000)

    async with db.connection() as conn:
        symbol = await market_repo.get_market_symbol(conn, symbol_code)
        if not symbol:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        if config["duration_ms"] is None:
            from_ts = 0
        else:
            from_ts = now_ms - int(config["duration_ms"])

        to_ts = now_ms

        candles = await market_repo.get_market_candles(
            conn,
            symbol_code=symbol_code,
            interval_key=interval_key,
            from_ts=from_ts,
            to_ts=to_ts,
        )

        seed_candle = await market_repo.get_last_market_candle_before(
            conn,
            symbol_code=symbol_code,
            interval_key=interval_key,
            before_ts=from_ts,
        )

    candles = densify_candles(
        candles=candles,
        interval_key=interval_key,
        from_ts=from_ts,
        to_ts=to_ts,
        seed_candle=seed_candle,
    )

    stats = compute_chart_stats(candles)

    return {
        "symbol": symbol,
        "range": range_key,
        "interval": interval_key,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "stats": stats,
        "candles": candles,
    }
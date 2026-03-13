from typing import Any, Dict, Optional


DEFAULT_MARKET_CONFIG = {
    "quote_strategy": "hybrid",
    "candle_strategy": "trades_only",
    "transaction_lookback_ms": 30 * 24 * 60 * 60 * 1000,
    "shop_stale_after_ms": 15 * 60 * 1000,
    "enabled_intervals": ["1m", "5m", "15m", "1h", "4h", "1d"],
    "carry_forward_enabled": True,
    "min_trade_count": 1,
    "min_trade_volume": 0,
    "outlier_filter_enabled": False,
}


async def get_market_config(conn) -> Dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT config_key, config_value
        FROM market_processor_config
        """
    )

    result = dict(DEFAULT_MARKET_CONFIG)

    for row in rows:
        result[row["config_key"]] = row["config_value"]

    return result


async def get_symbol_config(conn, symbol_code: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT
            symbol_code,
            is_enabled,
            quote_strategy,
            candle_strategy,
            transaction_lookback_ms,
            shop_stale_after_ms,
            min_trade_count,
            min_trade_volume,
            outlier_filter_enabled,
            carry_forward_enabled,
            enabled_intervals,
            extra_config,
            updated_at
        FROM market_symbol_config
        WHERE symbol_code = $1
        """,
        symbol_code,
    )

    return dict(row) if row else None
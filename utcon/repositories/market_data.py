from typing import Any, Dict, List, Optional


async def upsert_market_quote(conn, payload: Dict[str, Any]) -> None:
    await conn.execute(
        """
        INSERT INTO market_quotes (
            symbol_code,
            as_of_ts,
            last_trade_price,
            last_trade_ts,
            best_bid,
            best_bid_ts,
            best_ask,
            best_ask_ts,
            mid_price,
            mark_price,
            previous_close,
            session_open,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11, $12, NOW()
        )
        ON CONFLICT (symbol_code)
        DO UPDATE SET
            as_of_ts = EXCLUDED.as_of_ts,
            last_trade_price = EXCLUDED.last_trade_price,
            last_trade_ts = EXCLUDED.last_trade_ts,
            best_bid = EXCLUDED.best_bid,
            best_bid_ts = EXCLUDED.best_bid_ts,
            best_ask = EXCLUDED.best_ask,
            best_ask_ts = EXCLUDED.best_ask_ts,
            mid_price = EXCLUDED.mid_price,
            mark_price = EXCLUDED.mark_price,
            previous_close = EXCLUDED.previous_close,
            session_open = EXCLUDED.session_open,
            updated_at = NOW()
        """,
        payload["symbol_code"],
        payload["as_of_ts"],
        payload.get("last_trade_price"),
        payload.get("last_trade_ts"),
        payload.get("best_bid"),
        payload.get("best_bid_ts"),
        payload.get("best_ask"),
        payload.get("best_ask_ts"),
        payload.get("mid_price"),
        payload.get("mark_price"),
        payload.get("previous_close"),
        payload.get("session_open"),
    )


async def upsert_market_quote_sample(conn, payload: Dict[str, Any]) -> None:
    await conn.execute(
        """
        INSERT INTO market_quote_samples (
            symbol_code,
            sample_ts,
            last_trade_price,
            best_bid,
            best_ask,
            mid_price,
            microprice,
            mark_price,
            bid_liquidity,
            ask_liquidity,
            trade_count_delta,
            trade_volume_delta,
            source_trade_count,
            source_shop_count,
            is_synthetic,
            created_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15, NOW()
        )
        ON CONFLICT (symbol_code, sample_ts)
        DO UPDATE SET
            last_trade_price = EXCLUDED.last_trade_price,
            best_bid = EXCLUDED.best_bid,
            best_ask = EXCLUDED.best_ask,
            mid_price = EXCLUDED.mid_price,
            microprice = EXCLUDED.microprice,
            mark_price = EXCLUDED.mark_price,
            bid_liquidity = EXCLUDED.bid_liquidity,
            ask_liquidity = EXCLUDED.ask_liquidity,
            trade_count_delta = EXCLUDED.trade_count_delta,
            trade_volume_delta = EXCLUDED.trade_volume_delta,
            source_trade_count = EXCLUDED.source_trade_count,
            source_shop_count = EXCLUDED.source_shop_count,
            is_synthetic = EXCLUDED.is_synthetic
        """,
        payload["symbol_code"],
        payload["sample_ts"],
        payload.get("last_trade_price"),
        payload.get("best_bid"),
        payload.get("best_ask"),
        payload.get("mid_price"),
        payload.get("microprice"),
        payload.get("mark_price"),
        payload.get("bid_liquidity", 0.0),
        payload.get("ask_liquidity", 0.0),
        payload.get("trade_count_delta", 0),
        payload.get("trade_volume_delta", 0.0),
        payload.get("source_trade_count", 0),
        payload.get("source_shop_count", 0),
        payload.get("is_synthetic", False),
    )


async def get_market_quote_samples(
    conn,
    symbol_code: str,
    from_ts: int,
    to_ts: int,
    limit: Optional[int] = None,
    newest_first: bool = False,
) -> List[Dict[str, Any]]:
    order_sql = "DESC" if newest_first else "ASC"

    limit_sql = ""
    params: List[Any] = [symbol_code, from_ts, to_ts]

    if limit is not None:
        params.append(limit)
        limit_sql = f"LIMIT ${len(params)}"

    rows = await conn.fetch(
        f"""
        SELECT
            symbol_code,
            sample_ts,
            last_trade_price,
            best_bid,
            best_ask,
            mid_price,
            microprice,
            mark_price,
            bid_liquidity,
            ask_liquidity,
            trade_count_delta,
            trade_volume_delta,
            source_trade_count,
            source_shop_count,
            is_synthetic,
            created_at
        FROM market_quote_samples
        WHERE symbol_code = $1
          AND sample_ts >= $2
          AND sample_ts <= $3
        ORDER BY sample_ts {order_sql}
        {limit_sql}
        """,
        *params,
    )

    return [dict(row) for row in rows]


async def upsert_market_candles(
    conn,
    symbol_code: str,
    interval_key: str,
    candles: List[Dict[str, Any]],
) -> int:
    count = 0

    for candle in candles:
        await conn.execute(
            """
            INSERT INTO market_candles (
                symbol_code,
                interval_key,
                bucket_start_ts,
                bucket_end_ts,
                open,
                high,
                low,
                close,
                vwap,
                median,
                trade_volume,
                trade_count,
                buy_volume,
                sell_volume,
                best_bid,
                best_ask,
                midpoint,
                source_trade_count,
                source_shop_count,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, NOW()
            )
            ON CONFLICT (symbol_code, interval_key, bucket_start_ts)
            DO UPDATE SET
                bucket_end_ts = EXCLUDED.bucket_end_ts,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                vwap = EXCLUDED.vwap,
                median = EXCLUDED.median,
                trade_volume = EXCLUDED.trade_volume,
                trade_count = EXCLUDED.trade_count,
                buy_volume = EXCLUDED.buy_volume,
                sell_volume = EXCLUDED.sell_volume,
                best_bid = EXCLUDED.best_bid,
                best_ask = EXCLUDED.best_ask,
                midpoint = EXCLUDED.midpoint,
                source_trade_count = EXCLUDED.source_trade_count,
                source_shop_count = EXCLUDED.source_shop_count,
                updated_at = NOW()
            """,
            symbol_code,
            interval_key,
            candle["bucket_start_ts"],
            candle["bucket_end_ts"],
            candle.get("open"),
            candle.get("high"),
            candle.get("low"),
            candle.get("close"),
            candle.get("vwap"),
            candle.get("median"),
            candle.get("trade_volume", 0.0),
            candle.get("trade_count", 0),
            candle.get("buy_volume", 0.0),
            candle.get("sell_volume", 0.0),
            candle.get("best_bid"),
            candle.get("best_ask"),
            candle.get("midpoint"),
            candle.get("source_trade_count", 0),
            candle.get("source_shop_count", 0),
        )
        count += 1

    return count


async def get_market_quote(conn, symbol_code: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT
            mq.symbol_code,
            mq.as_of_ts,
            mq.last_trade_price,
            mq.last_trade_ts,
            mq.best_bid,
            mq.best_bid_ts,
            mq.best_ask,
            mq.best_ask_ts,
            mq.mid_price,
            mq.mark_price,
            mq.previous_close,
            mq.session_open,
            mq.updated_at,
            ms.name AS symbol_name,
            ms.description AS symbol_description,
            ms.pricing_method,
            ms.display_price_source
        FROM market_quotes mq
        JOIN market_symbols ms
          ON ms.code = mq.symbol_code
        WHERE mq.symbol_code = $1
        """,
        symbol_code,
    )

    return dict(row) if row else None


async def get_market_symbol(conn, symbol_code: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT
            code,
            name,
            description,
            pricing_method,
            display_price_source,
            is_active,
            created_at,
            updated_at
        FROM market_symbols
        WHERE code = $1
        """,
        symbol_code,
    )

    return dict(row) if row else None


async def get_market_candles(
    conn,
    symbol_code: str,
    interval_key: str,
    from_ts: int,
    to_ts: int,
) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            symbol_code,
            interval_key,
            bucket_start_ts,
            bucket_end_ts,
            open,
            high,
            low,
            close,
            vwap,
            median,
            trade_volume,
            trade_count,
            buy_volume,
            sell_volume,
            best_bid,
            best_ask,
            midpoint,
            source_trade_count,
            source_shop_count,
            updated_at
        FROM market_candles
        WHERE symbol_code = $1
          AND interval_key = $2
          AND bucket_start_ts >= $3
          AND bucket_start_ts <= $4
        ORDER BY bucket_start_ts ASC
        """,
        symbol_code,
        interval_key,
        from_ts,
        to_ts,
    )

    return [dict(row) for row in rows]


async def get_last_market_candle_before(
    conn,
    symbol_code: str,
    interval_key: str,
    before_ts: int,
) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT
            symbol_code,
            interval_key,
            bucket_start_ts,
            bucket_end_ts,
            open,
            high,
            low,
            close,
            vwap,
            median,
            trade_volume,
            trade_count,
            buy_volume,
            sell_volume,
            best_bid,
            best_ask,
            midpoint,
            source_trade_count,
            source_shop_count,
            updated_at
        FROM market_candles
        WHERE symbol_code = $1
          AND interval_key = $2
          AND bucket_start_ts < $3
        ORDER BY bucket_start_ts DESC
        LIMIT 1
        """,
        symbol_code,
        interval_key,
        before_ts,
    )

    return dict(row) if row else None

async def get_market_cap_snapshot(
    conn,
    items: List[Dict[str, Any]],
    *,
    last_seen_since_ts: int | None = None,
) -> Dict[str, Any]:
    total_known_volume = 0.0
    total_raw_units = 0.0
    shop_ids: set[int] = set()

    for item in items:
        clauses = ["item_type = $1", "remaining > 0"]
        params: List[Any] = [item["item_type"]]

        snbt = item.get("snbt")
        item_name = item.get("item_name")

        if snbt not in (None, "", "{}"):
            params.append(snbt)
            clauses.append(f"snbt = ${len(params)}")
        elif item_name:
            params.append(item_name)
            clauses.append(f"(item_name = ${len(params)} OR item_name IS NULL)")

        if last_seen_since_ts is not None:
            params.append(last_seen_since_ts)
            clauses.append(f"last_seen >= ${len(params)}")

        row = await conn.fetchrow(
            f"""
            SELECT
                COALESCE(SUM(remaining * item_quantity), 0) AS raw_units,
                COALESCE(SUM(remaining * item_quantity * ${{len(params) + 1}}), 0) AS known_volume,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT shop_id), NULL) AS shop_ids
            FROM shops
            WHERE {' AND '.join(clauses)}
            """,
            *params,
            float(item.get("quantity_multiplier") or 1.0),
        )

        raw_units = float(row["raw_units"] or 0)
        known_volume = float(row["known_volume"] or 0)

        total_raw_units += raw_units
        total_known_volume += known_volume
        shop_ids.update(int(shop_id) for shop_id in (row["shop_ids"] or []))

    return {
        "raw_units": total_raw_units,
        "known_volume": total_known_volume,
        "shop_count": len(shop_ids),
    }

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any


ZERO = Decimal("0")
ONE = Decimal("1")
DEFAULT_BINARY_PRICE = Decimal("0.5")


def _to_decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    if value is None:
        return default
    return Decimal(str(value))


def _normalize_status_for_filter(status: str | None, include_closed: bool) -> tuple[str | None, bool]:
    normalized = (status or "").strip().lower() or None
    return normalized, include_closed


async def get_market(conn, market_code: str):
    return await conn.fetchrow(
        """
        SELECT *
        FROM prediction_markets
        WHERE code = $1
        """,
        market_code,
    )


async def get_market_options(conn, market_code: str):
    return await conn.fetch(
        """
        SELECT
            o.id,
            o.market_code,
            o.option_code,
            o.label,
            o.description,
            o.sort_order,
            o.is_active,
            o.is_resolved_winner,
            o.numeric_value,
            o.range_min,
            o.range_max,
            o.range_min_inclusive,
            o.range_max_inclusive,
            COALESCE(s.pool_amount, 0) AS pool_amount,
            COALESCE(s.implied_price, 0) AS implied_price,
            COALESCE(s.trade_volume, 0) AS trade_volume,
            COALESCE(s.wager_count, 0) AS wager_count,
            s.last_trade_ts,
            s.updated_at AS state_updated_at
        FROM prediction_market_options o
        LEFT JOIN prediction_option_state s
            ON s.option_id = o.id
           AND s.market_code = o.market_code
        WHERE o.market_code = $1
        ORDER BY o.sort_order ASC, o.id ASC
        """,
        market_code,
    )


async def get_recent_wagers(conn, market_code: str, limit: int = 8):
    return await conn.fetch(
        """
        SELECT
            pw.id,
            pw.discord_uuid,
            pw.market_code,
            pw.option_id,
            pw.amount,
            pw.price_before,
            pw.price_after,
            pw.payout_amount,
            pw.gross_payout_amount,
            pw.profit_amount,
            pw.fee_amount,
            pw.outcome,
            pw.created_at,
            pw.settled_at,
            pmo.option_code,
            pmo.label AS option_label
        FROM prediction_wagers pw
        LEFT JOIN prediction_market_options pmo
            ON pmo.id = pw.option_id
        WHERE pw.market_code = $1
        ORDER BY pw.created_at DESC, pw.id DESC
        LIMIT $2
        """,
        market_code,
        limit,
    )


async def get_user_wagers(conn, discord_uuid: str, unsettled_only: bool = True, limit: int = 25):
    if unsettled_only:
        query = """
            SELECT
                pw.*,
                pm.title AS market_title,
                pm.status AS market_status,
                pm.outcome AS market_outcome,
                pm.closes_at,
                pmo.option_code,
                pmo.label AS option_label
            FROM prediction_wagers pw
            JOIN prediction_markets pm
              ON pm.code = pw.market_code
            LEFT JOIN prediction_market_options pmo
              ON pmo.id = pw.option_id
            WHERE pw.discord_uuid = $1
              AND pw.settled_at IS NULL
            ORDER BY pw.created_at DESC, pw.id DESC
            LIMIT $2
        """
    else:
        query = """
            SELECT
                pw.*,
                pm.title AS market_title,
                pm.status AS market_status,
                pm.outcome AS market_outcome,
                pm.closes_at,
                pmo.option_code,
                pmo.label AS option_label
            FROM prediction_wagers pw
            JOIN prediction_markets pm
              ON pm.code = pw.market_code
            LEFT JOIN prediction_market_options pmo
              ON pmo.id = pw.option_id
            WHERE pw.discord_uuid = $1
            ORDER BY pw.created_at DESC, pw.id DESC
            LIMIT $2
        """
    return await conn.fetch(query, discord_uuid, limit)


async def _market_total_volume_from_options(conn, market_code: str) -> Decimal:
    value = await conn.fetchval(
        """
        SELECT COALESCE(SUM(pool_amount), 0)
        FROM prediction_option_state
        WHERE market_code = $1
        """,
        market_code,
    )
    return _to_decimal(value)


def _serialize_option(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "market_code": row["market_code"],
        "option_code": row["option_code"],
        "label": row["label"],
        "description": row["description"],
        "sort_order": row["sort_order"],
        "is_active": row["is_active"],
        "is_resolved_winner": row["is_resolved_winner"],
        "numeric_value": row["numeric_value"],
        "range_min": row["range_min"],
        "range_max": row["range_max"],
        "range_min_inclusive": row["range_min_inclusive"],
        "range_max_inclusive": row["range_max_inclusive"],
        "pool_amount": _to_decimal(row["pool_amount"]),
        "implied_price": _to_decimal(row["implied_price"]),
        "trade_volume": _to_decimal(row["trade_volume"]),
        "wager_count": int(row["wager_count"] or 0),
        "last_trade_ts": row["last_trade_ts"],
        "updated_at": row["state_updated_at"],
    }


def _serialize_recent_wager(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "discord_uuid": row["discord_uuid"],
        "market_code": row["market_code"],
        "option_id": row["option_id"],
        "option_code": row["option_code"],
        "option_label": row["option_label"],
        "amount": _to_decimal(row["amount"]),
        "price_before": _to_decimal(row["price_before"]),
        "price_after": _to_decimal(row["price_after"]),
        "payout_amount": _to_decimal(row["payout_amount"]),
        "gross_payout_amount": _to_decimal(row["gross_payout_amount"]),
        "profit_amount": _to_decimal(row["profit_amount"]),
        "fee_amount": _to_decimal(row["fee_amount"]),
        "outcome": row["outcome"],
        "created_at": row["created_at"],
        "settled_at": row["settled_at"],
    }


async def build_market_payload(conn, market_code: str) -> dict[str, Any]:
    market = await get_market(conn, market_code)
    if market is None:
        raise LookupError("market_not_found")

    option_rows = await get_market_options(conn, market_code)
    options = [_serialize_option(row) for row in option_rows]
    total_volume = await _market_total_volume_from_options(conn, market_code)
    recent_wagers = [_serialize_recent_wager(row) for row in await get_recent_wagers(conn, market_code)]

    yes_opt = next((opt for opt in options if opt["option_code"] == "YES"), None)
    no_opt = next((opt for opt in options if opt["option_code"] == "NO"), None)

    market_payload = {
        "code": market["code"],
        "title": market["title"],
        "description": market["description"],
        "market_type": market["market_type"],
        "resolution_mode": market["resolution_mode"],
        "status": market["status"],
        "outcome": market["outcome"],
        "created_by": market["created_by"],
        "created_at": market["created_at"],
        "updated_at": market["updated_at"],
        "closes_at": market["closes_at"],
        "resolves_at": market["resolves_at"],
        "resolution_notes": market["resolution_notes"],
        "winning_option_id": market["winning_option_id"],
        "winning_numeric_value": market["winning_numeric_value"],
        "total_volume": total_volume,
        "yes_pool": yes_opt["pool_amount"] if yes_opt else ZERO,
        "no_pool": no_opt["pool_amount"] if no_opt else ZERO,
        "price_yes": yes_opt["implied_price"] if yes_opt else DEFAULT_BINARY_PRICE,
        "price_no": no_opt["implied_price"] if no_opt else DEFAULT_BINARY_PRICE,
        "last_trade_ts": max((opt["last_trade_ts"] or 0) for opt in options) if options else None,
    }

    return {
        "market": market_payload,
        "options": options,
        "recent_wagers": recent_wagers,
    }


async def list_markets(conn, status: str | None = None, include_closed: bool = False, limit: int = 25):
    status, include_closed = _normalize_status_for_filter(status, include_closed)

    if include_closed:
        if status:
            rows = await conn.fetch(
                """
                SELECT code
                FROM prediction_markets
                WHERE status = $1
                ORDER BY created_at DESC, code ASC
                LIMIT $2
                """,
                status,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT code
                FROM prediction_markets
                ORDER BY created_at DESC, code ASC
                LIMIT $1
                """,
                limit,
            )
    else:
        effective_status = status or "open"
        rows = await conn.fetch(
            """
            SELECT code
            FROM prediction_markets
            WHERE status = $1
            ORDER BY created_at DESC, code ASC
            LIMIT $2
            """,
            effective_status,
            limit,
        )

    items: list[dict[str, Any]] = []
    for row in rows:
        payload = await build_market_payload(conn, row["code"])
        market = payload["market"]
        items.append(
            {
                "code": market["code"],
                "title": market["title"],
                "description": market["description"],
                "market_type": market["market_type"],
                "status": market["status"],
                "outcome": market["outcome"],
                "closes_at": market["closes_at"],
                "resolves_at": market["resolves_at"],
                "created_at": market["created_at"],
                "updated_at": market["updated_at"],
                "total_volume": market["total_volume"],
                "yes_pool": market["yes_pool"],
                "no_pool": market["no_pool"],
                "price_yes": market["price_yes"],
                "price_no": market["price_no"],
                "options": payload["options"],
            }
        )
    return items


async def create_market(conn, req) -> dict[str, Any]:
    market_code = req.code.strip().upper()
    existing = await get_market(conn, market_code)
    if existing is not None:
        raise ValueError("market_code_already_exists")

    await conn.execute(
        """
        INSERT INTO prediction_markets (
            code,
            title,
            description,
            market_type,
            resolution_mode,
            status,
            closes_at,
            resolves_at,
            created_by
        )
        VALUES ($1,$2,$3,$4,$5,'open',$6,$7,$8)
        """,
        market_code,
        req.title,
        req.description,
        req.market_type,
        req.resolution_mode,
        req.closes_at,
        req.resolves_at,
        req.created_by,
    )

    option_models = list(req.options or [])
    if not option_models:
        option_models = [
            type("Opt", (), {"option_code": "YES", "label": "YES", "sort_order": 10, "description": None, "numeric_value": None, "range_min": None, "range_max": None, "range_min_inclusive": True, "range_max_inclusive": False})(),
            type("Opt", (), {"option_code": "NO", "label": "NO", "sort_order": 20, "description": None, "numeric_value": None, "range_min": None, "range_max": None, "range_min_inclusive": True, "range_max_inclusive": False})(),
        ]

    option_count = len(option_models)
    initial_price = (ONE / Decimal(option_count)).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP) if option_count > 0 else ZERO

    for opt in option_models:
        option_code = str(opt.option_code).strip().upper()
        label = str(opt.label).strip()
        sort_order = int(getattr(opt, "sort_order", 0) or 0)

        row = await conn.fetchrow(
            """
            INSERT INTO prediction_market_options (
                market_code,
                option_code,
                label,
                description,
                sort_order,
                numeric_value,
                range_min,
                range_max,
                range_min_inclusive,
                range_max_inclusive
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            RETURNING id
            """,
            market_code,
            option_code,
            label,
            getattr(opt, "description", None),
            sort_order,
            getattr(opt, "numeric_value", None),
            getattr(opt, "range_min", None),
            getattr(opt, "range_max", None),
            getattr(opt, "range_min_inclusive", True),
            getattr(opt, "range_max_inclusive", False),
        )
        option_id = int(row["id"])

        await conn.execute(
            """
            INSERT INTO prediction_option_state (
                market_code,
                option_id,
                pool_amount,
                implied_price,
                trade_volume,
                wager_count
            )
            VALUES ($1,$2,0,$3,0,0)
            """,
            market_code,
            option_id,
            initial_price if option_count > 0 else ZERO,
        )

    return await build_market_payload(conn, market_code)


async def _recompute_option_state_prices(conn, market_code: str) -> None:
    rows = await conn.fetch(
        """
        SELECT option_id, pool_amount
        FROM prediction_option_state
        WHERE market_code = $1
        ORDER BY option_id ASC
        """,
        market_code,
    )
    if not rows:
        return

    total_pool = sum((_to_decimal(row["pool_amount"]) for row in rows), ZERO)
    count = len(rows)

    if total_pool <= ZERO:
        equal_price = (ONE / Decimal(count)).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        for row in rows:
            await conn.execute(
                """
                UPDATE prediction_option_state
                SET implied_price = $1,
                    updated_at = now()
                WHERE market_code = $2
                  AND option_id = $3
                """,
                equal_price,
                market_code,
                row["option_id"],
            )
        return

    for row in rows:
        price = (_to_decimal(row["pool_amount"]) / total_pool).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        await conn.execute(
            """
            UPDATE prediction_option_state
            SET implied_price = $1,
                updated_at = now()
            WHERE market_code = $2
              AND option_id = $3
            """,
            price,
            market_code,
            row["option_id"],
        )


async def _insert_option_snapshot_rows(conn, market_code: str) -> None:
    option_rows = await get_market_options(conn, market_code)
    yes_opt = next((opt for opt in option_rows if opt["option_code"] == "YES"), None)
    no_opt = next((opt for opt in option_rows if opt["option_code"] == "NO"), None)
    total_volume = sum((_to_decimal(row["pool_amount"]) for row in option_rows), ZERO)
    snapshot_ts = await conn.fetchval("SELECT (EXTRACT(EPOCH FROM now()) * 1000)::bigint")

    snapshot_id = await conn.fetchval(
        """
        INSERT INTO prediction_market_snapshots (
            market_code,
            snapshot_ts,
            yes_pool,
            no_pool,
            total_volume,
            price_yes,
            price_no
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        RETURNING id
        """,
        market_code,
        snapshot_ts,
        _to_decimal(yes_opt["pool_amount"]) if yes_opt else ZERO,
        _to_decimal(no_opt["pool_amount"]) if no_opt else ZERO,
        total_volume,
        _to_decimal(yes_opt["implied_price"]) if yes_opt else ZERO,
        _to_decimal(no_opt["implied_price"]) if no_opt else ZERO,
    )

    await conn.execute(
        """
        INSERT INTO prediction_option_snapshots (
            market_snapshot_id,
            market_code,
            option_id,
            snapshot_ts,
            pool_amount,
            implied_price,
            trade_volume,
            wager_count,
            created_at
        )
        SELECT
            $1,
            s.market_code,
            s.option_id,
            $2,
            s.pool_amount,
            s.implied_price,
            s.trade_volume,
            s.wager_count,
            now()
        FROM prediction_option_state s
        WHERE s.market_code = $3
        """,
        snapshot_id,
        snapshot_ts,
        market_code,
    )


async def place_wager(conn, req) -> dict[str, Any]:
    market_code = req.market_code.strip().upper()
    option_code = req.option_code.strip().upper()
    amount = _to_decimal(req.amount)

    if amount <= ZERO:
        raise ValueError("invalid_amount")

    market = await get_market(conn, market_code)
    if market is None:
        raise LookupError("market_not_found")

    status = str(market["status"] or "").strip().lower()
    if status != "open":
        raise ValueError("market_closed")

    closes_at = market["closes_at"]
    now_row = await conn.fetchrow(
        """
        SELECT
            (EXTRACT(EPOCH FROM now()) * 1000)::bigint AS now_ts,
            now() AS now_at
        """
    )
    now_ts = int(now_row["now_ts"])
    now_at = now_row["now_at"]

    market_closed_by_time = False
    if closes_at is not None:
        if isinstance(closes_at, (int, float)):
            market_closed_by_time = now_ts >= int(closes_at)
        else:
            market_closed_by_time = now_at >= closes_at

    if market_closed_by_time:
        await conn.execute(
            """
            UPDATE prediction_markets
            SET status = 'closed',
                updated_at = now()
            WHERE code = $1
              AND status = 'open'
            """,
            market_code,
        )
        raise ValueError("market_closed")

    option = await conn.fetchrow(
        """
        SELECT o.*, s.pool_amount, s.implied_price
        FROM prediction_market_options o
        JOIN prediction_option_state s
          ON s.market_code = o.market_code
         AND s.option_id = o.id
        WHERE o.market_code = $1
          AND o.option_code = $2
          AND o.is_active = true
        """,
        market_code,
        option_code,
    )
    if option is None:
        raise LookupError("option_not_found")

    balance = await conn.fetchrow(
        """
        SELECT balance
        FROM balances
        WHERE discord_uuid = $1
        """,
        req.discord_uuid,
    )
    if balance is None:
        raise ValueError("balance_not_found")
    if _to_decimal(balance["balance"]) < amount:
        raise ValueError("insufficient_balance")

    price_before = _to_decimal(option["implied_price"])
    if price_before <= ZERO:
        option_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM prediction_market_options
            WHERE market_code = $1
              AND is_active = true
            """,
            market_code,
        )
        option_count = int(option_count or 1)
        price_before = (ONE / Decimal(option_count)).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

    shares_received = (amount / price_before).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

    await conn.execute(
        """
        UPDATE balances
        SET balance = balance - $1,
            last_updated = now()
        WHERE discord_uuid = $2
        """,
        amount,
        req.discord_uuid,
    )

    await conn.execute(
        """
        INSERT INTO balance_transactions (
            discord_uuid,
            kind,
            amount,
            metadata
        )
        VALUES ($1, 'prediction_wager', $2, $3::jsonb)
        """,
        req.discord_uuid,
        -amount,
        (
            '{"market_code":"%s","option_code":"%s"}'
            % (market_code, option_code)
        ),
    )

    await conn.execute(
        """
        UPDATE prediction_option_state
        SET pool_amount = pool_amount + $1,
            trade_volume = trade_volume + $1,
            wager_count = wager_count + 1,
            last_trade_ts = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
            updated_at = now()
        WHERE market_code = $2
          AND option_id = $3
        """,
        amount,
        market_code,
        option["id"],
    )

    await _recompute_option_state_prices(conn, market_code)

    price_after = await conn.fetchval(
        """
        SELECT implied_price
        FROM prediction_option_state
        WHERE market_code = $1
          AND option_id = $2
        """,
        market_code,
        option["id"],
    )
    price_after = _to_decimal(price_after)

    # Legacy schema compatibility:
    # prediction_wagers.side, price_yes_before, price_yes_after are still NOT NULL.
    # For non-binary markets we store the chosen option code in side and mirror the
    # selected option's before/after prices into the legacy yes fields so inserts succeed.
    await conn.execute(
        """
        INSERT INTO prediction_wagers (
            market_code,
            discord_uuid,
            side,
            option_id,
            amount,
            price_yes_before,
            price_yes_after,
            price_before,
            price_after,
            shares_received
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        """,
        market_code,
        req.discord_uuid,
        option_code,
        option["id"],
        amount,
        price_before,
        price_after,
        price_before,
        price_after,
        shares_received,
    )

    await conn.execute(
        """
        UPDATE prediction_markets
        SET total_volume = COALESCE(total_volume, 0) + $1,
            updated_at = now(),
            last_trade_ts = (EXTRACT(EPOCH FROM now()) * 1000)::bigint
        WHERE code = $2
        """,
        amount,
        market_code,
    )

    await _insert_option_snapshot_rows(conn, market_code)

    payload = await build_market_payload(conn, market_code)
    balance_after = await conn.fetchval(
        """
        SELECT balance
        FROM balances
        WHERE discord_uuid = $1
        """,
        req.discord_uuid,
    )
    payload["balance_after"] = _to_decimal(balance_after)
    return payload


async def close_market(conn, market_code: str, closed_by: str | None = None) -> dict[str, Any]:
    market = await get_market(conn, market_code)
    if market is None:
        raise LookupError("market_not_found")

    await conn.execute(
        """
        UPDATE prediction_markets
        SET status = 'closed',
            updated_at = now()
        WHERE code = $1
        """,
        market_code,
    )
    return await build_market_payload(conn, market_code)


async def cancel_market(conn, market_code: str, cancelled_by: str | None = None, reason: str | None = None) -> dict[str, Any]:
    market = await get_market(conn, market_code)
    if market is None:
        raise LookupError("market_not_found")

    await conn.execute(
        """
        UPDATE prediction_markets
        SET status = 'cancelled',
            resolution_notes = COALESCE($2, resolution_notes),
            updated_at = now()
        WHERE code = $1
        """,
        market_code,
        reason,
    )
    return await build_market_payload(conn, market_code)


async def resolve_market(conn, market_code: str, option_code: str, resolved_by: str | None = None, resolution_notes: str | None = None) -> dict[str, Any]:
    market = await get_market(conn, market_code)
    if market is None:
        raise LookupError("market_not_found")

    option = await conn.fetchrow(
        """
        SELECT *
        FROM prediction_market_options
        WHERE market_code = $1
          AND option_code = $2
          AND is_active = true
        """,
        market_code,
        option_code.strip().upper(),
    )
    if option is None:
        raise LookupError("option_not_found")

    await conn.execute(
        """
        UPDATE prediction_market_options
        SET is_resolved_winner = false,
            updated_at = now()
        WHERE market_code = $1
        """,
        market_code,
    )

    await conn.execute(
        """
        UPDATE prediction_market_options
        SET is_resolved_winner = true,
            updated_at = now()
        WHERE id = $1
        """,
        option["id"],
    )

    await conn.execute(
        """
        UPDATE prediction_markets
        SET status = 'resolved',
            outcome = $2,
            winning_option_id = $3,
            resolution_notes = COALESCE($4, resolution_notes),
            updated_at = now()
        WHERE code = $1
        """,
        market_code,
        option["option_code"],
        option["id"],
        resolution_notes,
    )

    return await build_market_payload(conn, market_code)


async def resolve_market_by_numeric_result(
    conn,
    market_code: str,
    numeric_value: Decimal,
    resolved_by: str | None = None,
    resolution_notes: str | None = None,
) -> dict[str, Any]:
    market = await get_market(conn, market_code)
    if market is None:
        raise LookupError("market_not_found")

    options = await conn.fetch(
        """
        SELECT *
        FROM prediction_market_options
        WHERE market_code = $1
          AND is_active = true
        ORDER BY sort_order ASC, id ASC
        """,
        market_code,
    )

    winner = None
    numeric_value = _to_decimal(numeric_value)

    for option in options:
        if option["numeric_value"] is not None and _to_decimal(option["numeric_value"]) == numeric_value:
            winner = option
            break

        lower_ok = True
        upper_ok = True

        if option["range_min"] is not None:
            lower = _to_decimal(option["range_min"])
            lower_ok = numeric_value >= lower if option["range_min_inclusive"] else numeric_value > lower

        if option["range_max"] is not None:
            upper = _to_decimal(option["range_max"])
            upper_ok = numeric_value <= upper if option["range_max_inclusive"] else numeric_value < upper

        if lower_ok and upper_ok and (option["range_min"] is not None or option["range_max"] is not None):
            winner = option
            break

    if winner is None:
        raise ValueError("no_matching_option_for_numeric_result")

    await conn.execute(
        """
        UPDATE prediction_markets
        SET winning_numeric_value = $2,
            updated_at = now()
        WHERE code = $1
        """,
        market_code,
        numeric_value,
    )

    return await resolve_market(
        conn,
        market_code=market_code,
        option_code=winner["option_code"],
        resolved_by=resolved_by,
        resolution_notes=resolution_notes,
    )


async def list_user_wagers(conn, discord_uuid: str, unsettled_only: bool = True, limit: int = 25):
    rows = await get_user_wagers(conn, discord_uuid, unsettled_only=unsettled_only, limit=limit)
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "id": row["id"],
                "market_code": row["market_code"],
                "market_title": row["market_title"],
                "market_status": row["market_status"],
                "market_outcome": row["market_outcome"],
                "closes_at": row["closes_at"],
                "discord_uuid": row["discord_uuid"],
                "option_id": row["option_id"],
                "option_code": row["option_code"],
                "option_label": row["option_label"],
                "side": row["option_code"],
                "amount": _to_decimal(row["amount"]),
                "price_before": _to_decimal(row["price_before"]),
                "price_after": _to_decimal(row["price_after"]),
                "payout_amount": _to_decimal(row["payout_amount"]),
                "gross_payout_amount": _to_decimal(row["gross_payout_amount"]),
                "profit_amount": _to_decimal(row["profit_amount"]),
                "fee_amount": _to_decimal(row["fee_amount"]),
                "created_at": row["created_at"],
                "settled_at": row["settled_at"],
                "outcome": row["outcome"],
            }
        )
    return items


async def get_market_history(conn, market_code: str):
    market = await get_market(conn, market_code)
    if market is None:
        raise LookupError("market_not_found")

    rows = await conn.fetch(
        """
        SELECT
            pos.market_code,
            pos.option_id,
            pmo.option_code,
            pmo.label AS option_label,
            pos.snapshot_ts,
            pos.pool_amount,
            pos.implied_price,
            pos.trade_volume,
            pos.wager_count
        FROM prediction_option_snapshots pos
        JOIN prediction_market_options pmo
          ON pmo.id = pos.option_id
        WHERE pos.market_code = $1
        ORDER BY pos.snapshot_ts ASC, pmo.sort_order ASC, pmo.id ASC
        """,
        market_code,
    )

    by_option: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = row["option_code"]
        bucket = by_option.setdefault(
            code,
            {
                "option_id": row["option_id"],
                "option_code": code,
                "label": row["option_label"],
                "points": [],
            },
        )
        bucket["points"].append(
            {
                "snapshot_ts": row["snapshot_ts"],
                "pool_amount": _to_decimal(row["pool_amount"]),
                "implied_price": _to_decimal(row["implied_price"]),
                "trade_volume": _to_decimal(row["trade_volume"]),
                "wager_count": int(row["wager_count"] or 0),
            }
        )

    return {
        "market": (await build_market_payload(conn, market_code))["market"],
        "series": list(by_option.values()),
    }


async def list_pending_settlements(conn, limit: int = 100):
    return []
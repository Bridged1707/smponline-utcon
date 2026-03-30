from __future__ import annotations

from decimal import Decimal


# =========================
# HELPERS
# =========================

async def get_market(conn, market_code: str):
    return await conn.fetchrow(
        "SELECT * FROM prediction_markets WHERE code = $1",
        market_code,
    )


async def get_market_options(conn, market_code: str):
    rows = await conn.fetch(
        """
        SELECT
            o.id,
            o.option_code,
            o.label,
            o.sort_order,
            o.is_active,
            o.is_resolved_winner,
            s.pool_amount,
            s.implied_price
        FROM prediction_market_options o
        LEFT JOIN prediction_option_state s
            ON s.option_id = o.id
        WHERE o.market_code = $1
        ORDER BY o.sort_order ASC
        """,
        market_code,
    )
    return rows


# =========================
# CREATE MARKET
# =========================

async def create_market(conn, req):
    await conn.execute(
        """
        INSERT INTO prediction_markets (
            code, title, description,
            market_type, resolution_mode,
            status, closes_at
        )
        VALUES ($1,$2,$3,$4,$5,'open',$6)
        """,
        req.code,
        req.title,
        req.description,
        req.market_type,
        req.resolution_mode,
        req.closes_at,
    )

    # auto YES/NO
    options = req.options
    if not options:
        options = [
            {"option_code": "YES", "label": "YES", "sort_order": 10},
            {"option_code": "NO", "label": "NO", "sort_order": 20},
        ]

    for opt in options:
        row = await conn.fetchrow(
            """
            INSERT INTO prediction_market_options
            (market_code, option_code, label, sort_order)
            VALUES ($1,$2,$3,$4)
            RETURNING id
            """,
            req.code,
            opt["option_code"],
            opt["label"],
            opt.get("sort_order", 0),
        )

        await conn.execute(
            """
            INSERT INTO prediction_option_state
            (market_code, option_id, pool_amount, implied_price)
            VALUES ($1,$2,0,0)
            """,
            req.code,
            row["id"],
        )


# =========================
# LIST / LOOKUP
# =========================

async def build_market_payload(conn, market_code: str):
    market = await get_market(conn, market_code)
    if not market:
        raise LookupError("market_not_found")

    options = await get_market_options(conn, market_code)

    total_volume = sum([o["pool_amount"] or 0 for o in options])

    return {
        "code": market["code"],
        "title": market["title"],
        "description": market["description"],
        "market_type": market["market_type"],
        "status": market["status"],
        "options": [
            {
                "id": o["id"],
                "option_code": o["option_code"],
                "label": o["label"],
                "pool_amount": o["pool_amount"] or 0,
                "implied_price": o["implied_price"] or 0,
            }
            for o in options
        ],
        "total_volume": total_volume,
    }


async def list_markets(conn):
    rows = await conn.fetch(
        "SELECT code FROM prediction_markets WHERE status='open'"
    )

    result = []
    for r in rows:
        result.append(await build_market_payload(conn, r["code"]))

    return result


# =========================
# WAGER
# =========================

async def place_wager(conn, req):
    market = await get_market(conn, req.market_code)
    if not market:
        raise LookupError("market_not_found")

    option = await conn.fetchrow(
        """
        SELECT * FROM prediction_market_options
        WHERE market_code=$1 AND option_code=$2
        """,
        req.market_code,
        req.option_code,
    )
    if not option:
        raise LookupError("option_not_found")

    await conn.execute(
        """
        INSERT INTO prediction_wagers
        (discord_uuid, market_code, option_id, amount)
        VALUES ($1,$2,$3,$4)
        """,
        req.discord_uuid,
        req.market_code,
        option["id"],
        req.amount,
    )

    # update pool
    await conn.execute(
        """
        UPDATE prediction_option_state
        SET pool_amount = pool_amount + $1
        WHERE option_id = $2
        """,
        req.amount,
        option["id"],
    )

    # recompute prices
    rows = await conn.fetch(
        """
        SELECT option_id, pool_amount
        FROM prediction_option_state
        WHERE market_code=$1
        """,
        req.market_code,
    )

    total = sum([r["pool_amount"] for r in rows]) or 1

    for r in rows:
        price = Decimal(r["pool_amount"]) / Decimal(total)
        await conn.execute(
            """
            UPDATE prediction_option_state
            SET implied_price=$1
            WHERE option_id=$2
            """,
            price,
            r["option_id"],
        )


# =========================
# RESOLVE
# =========================

async def resolve_market(conn, market_code: str, option_code: str):
    option = await conn.fetchrow(
        """
        SELECT * FROM prediction_market_options
        WHERE market_code=$1 AND option_code=$2
        """,
        market_code,
        option_code,
    )
    if not option:
        raise LookupError("option_not_found")

    await conn.execute(
        """
        UPDATE prediction_market_options
        SET is_resolved_winner=true
        WHERE id=$1
        """,
        option["id"],
    )

    await conn.execute(
        """
        UPDATE prediction_markets
        SET status='resolved', winning_option_id=$1
        WHERE code=$2
        """,
        option["id"],
        market_code,
    )
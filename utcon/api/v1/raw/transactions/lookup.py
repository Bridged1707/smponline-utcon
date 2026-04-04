from typing import Literal, Optional

from fastapi import APIRouter, Query

from utcon import db

router = APIRouter(prefix="/api/v1/raw/transactions", tags=["raw"])


@router.get("/lookup")
async def lookup_transactions(
    query: Optional[str] = None,
    item_type: Optional[str] = None,
    item_name: Optional[str] = None,
    snbt: Optional[str] = None,
    transaction_type: Optional[str] = None,
    event_type: Optional[str] = None,
    shop_world: Optional[str] = None,
    shop_x: Optional[int] = None,
    shop_y: Optional[int] = None,
    shop_z: Optional[int] = None,
    since_ts: Optional[int] = None,
    until_ts: Optional[int] = None,
    min_unit_price: Optional[float] = None,
    max_unit_price: Optional[float] = None,
    include_disabled: bool = False,
    limit: int = Query(default=100, ge=1, le=5000),
    order: Literal["asc", "desc"] = "desc",
    nbt_wildcard: Optional[str] = Query(None),
):
    where_clauses = []
    params = []

    def add_param(value):
        params.append(value)
        return f"${len(params)}"

    if not include_disabled:
        where_clauses.append("is_enabled = TRUE")

    if query is not None:
        normalized_query = query.strip()
        normalized_item_type = normalized_query.upper().replace(" ", "_")
        like_value = f"%{normalized_query}%"
        where_clauses.append(
            f"(item_type = {add_param(normalized_item_type)} OR item_name ILIKE {add_param(like_value)})"
        )

    if item_type is not None:
        where_clauses.append(f"item_type = {add_param(item_type)}")
    if item_name is not None:
        where_clauses.append(f"item_name = {add_param(item_name)}")
    if snbt is not None:
        where_clauses.append(f"snbt = {add_param(snbt)}")

    if nbt_wildcard is not None and nbt_wildcard.strip():
        wildcard_value = f"%{nbt_wildcard.strip()}%"
        where_clauses.append(f"snbt ILIKE {add_param(wildcard_value)}")

    if transaction_type is not None:
        where_clauses.append(f"transaction_type = {add_param(transaction_type)}")
    if event_type is not None:
        where_clauses.append(f"event = {add_param(event_type)}")
    if shop_world is not None:
        where_clauses.append(f"shop_world = {add_param(shop_world)}")
    if shop_x is not None:
        where_clauses.append(f"shop_x = {add_param(shop_x)}")
    if shop_y is not None:
        where_clauses.append(f"shop_y = {add_param(shop_y)}")
    if shop_z is not None:
        where_clauses.append(f"shop_z = {add_param(shop_z)}")
    if since_ts is not None:
        where_clauses.append(f"timestamp >= {add_param(since_ts)}")
    if until_ts is not None:
        where_clauses.append(f"timestamp <= {add_param(until_ts)}")
    if min_unit_price is not None:
        where_clauses.append(f"unit_price >= {add_param(min_unit_price)}")
    if max_unit_price is not None:
        where_clauses.append(f"unit_price <= {add_param(max_unit_price)}")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
        SELECT
            id,
            hash,
            event,
            timestamp,
            data,
            item_type,
            item_name,
            snbt,
            quantity,
            unit_price,
            total_price,
            currency_amount,
            shop_x,
            shop_y,
            shop_z,
            shop_world,
            transaction_type,
            is_enabled
        FROM transactions
        {where_sql}
        ORDER BY timestamp {order.upper()}, id {order.upper()}
        LIMIT {limit}
    """

    async with db.connection() as conn:
        rows = await conn.fetch(sql, *params)

    return {
        "items": [dict(row) for row in rows],
        "count": len(rows),
    }
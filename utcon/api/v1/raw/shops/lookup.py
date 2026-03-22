from typing import Literal, Optional

from fastapi import APIRouter, Query

from utcon import db

router = APIRouter(prefix="/api/v1/raw/shops", tags=["raw"])


@router.get("/lookup")
async def lookup_shops(
    query: Optional[str] = None,
    item_type: Optional[str] = None,
    item_name: Optional[str] = None,
    snbt: Optional[str] = None,
    shop_type: Optional[str] = None,
    active_only: bool = False,
    last_seen_since_ts: Optional[int] = None,
    exact_price: Optional[float] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    item_quantity: Optional[int] = None,
    min_remaining: Optional[int] = None,
    limit: int = Query(default=100, ge=1, le=5000),
    order_by: Literal["last_seen", "price", "shop_id"] = "last_seen",
    order: Literal["asc", "desc"] = "desc",
):
    where_clauses = []
    params = []

    def add_param(value):
        params.append(value)
        return f"${len(params)}"

    if query is not None:
        normalized_query = query.strip()
        normalized_item_type = normalized_query.upper().replace(" ", "_")
        like_value = f"%{normalized_query}%"
        where_clauses.append(
            f"(item_type = {add_param(normalized_item_type)} OR item_name ILIKE {add_param(like_value)} OR owner_name ILIKE {add_param(like_value)})"
        )
    if item_type is not None:
        where_clauses.append(f"item_type = {add_param(item_type)}")
    if item_name is not None:
        where_clauses.append(f"item_name = {add_param(item_name)}")
    if snbt is not None:
        where_clauses.append(f"snbt = {add_param(snbt)}")
    if shop_type is not None:
        where_clauses.append(f"shop_type = {add_param(shop_type)}")
    if active_only:
        where_clauses.append("remaining > 0")
    if last_seen_since_ts is not None:
        where_clauses.append(f"last_seen >= {add_param(last_seen_since_ts)}")
    if exact_price is not None:
        where_clauses.append(f"price = {add_param(exact_price)}")
    if min_price is not None:
        where_clauses.append(f"price >= {add_param(min_price)}")
    if max_price is not None:
        where_clauses.append(f"price <= {add_param(max_price)}")
    if item_quantity is not None:
        where_clauses.append(f"item_quantity = {add_param(item_quantity)}")
    if min_remaining is not None:
        where_clauses.append(f"remaining >= {add_param(min_remaining)}")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    order_columns = {
        "last_seen": "last_seen",
        "price": "price",
        "shop_id": "shop_id",
    }
    order_column = order_columns[order_by]

    sql = f"""
        SELECT
            shop_id,
            owner_name,
            owner_uuid,
            world,
            x,
            y,
            z,
            shop_type,
            price,
            remaining,
            item_type,
            item_name,
            item_quantity,
            snbt,
            last_seen
        FROM shops
        {where_sql}
        ORDER BY {order_column} {order.upper()}, shop_id {order.upper()}
        LIMIT {limit}
    """

    async with db.connection() as conn:
        rows = await conn.fetch(sql, *params)

    return {
        "items": [dict(row) for row in rows],
        "count": len(rows),
    }
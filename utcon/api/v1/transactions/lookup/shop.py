from fastapi import APIRouter
from app.core.database import get_pool

router = APIRouter(prefix="/lookup/shop")


@router.get("")
async def lookup_shop(item_type: str, limit: int = 50):

    pool = await get_pool()

    async with pool.acquire() as conn:

        rows = await conn.fetch(
            """
            SELECT *
            FROM transactions
            WHERE event = 'shopTransaction'
            AND data->'item'->>'type' = $1
            ORDER BY timestamp DESC
            LIMIT $2
            """,
            item_type,
            limit
        )

    return [dict(r) for r in rows]
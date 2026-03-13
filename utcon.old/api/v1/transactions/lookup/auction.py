from fastapi import APIRouter
from utcon.core.database import get_pool

router = APIRouter(prefix="/lookup/auction")


@router.get("")
async def lookup_auction(item_type: str, limit: int = 50):

    pool = await get_pool()

    async with pool.acquire() as conn:

        rows = await conn.fetch(
            """
            SELECT *
            FROM transactions
            WHERE event = 'auctionComplete'
            AND data->'item'->>'type' = $1
            ORDER BY timestamp DESC
            LIMIT $2
            """,
            item_type,
            limit
        )

    return [dict(r) for r in rows]
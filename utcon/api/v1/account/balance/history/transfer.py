from fastapi import APIRouter
from utcon import db

router = APIRouter(prefix="/v1/account/balance/history", tags=["balance"])


@router.get("/transfer")
async def transfer_history(discord_uuid: str, limit: int = 50):

    async with db.connection() as conn:

        rows = await conn.fetch(
            """
            SELECT *
            FROM balance_transfers
            WHERE discord_uuid=$1
            ORDER BY created_at DESC
            LIMIT $2
            """,
            discord_uuid,
            limit
        )

    return [dict(r) for r in rows]
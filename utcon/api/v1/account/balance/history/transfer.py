from fastapi import APIRouter
from utcon import db

router = APIRouter()


@router.get("/account/balance/history/transfer")
async def transfer_history(discord_uuid: str, limit: int = 50):

    async with db.connection() as conn:

        rows = await conn.fetch(
            """
            SELECT
                id,
                type,
                from_discord_uuid,
                to_discord_uuid,
                amount,
                created_at
            FROM transactions
            WHERE
                from_discord_uuid=$1
                OR to_discord_uuid=$1
            ORDER BY created_at DESC
            LIMIT $2
            """,
            discord_uuid,
            limit
        )

        return [dict(r) for r in rows]
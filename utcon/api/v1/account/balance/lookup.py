from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import balance as balance_repo

router = APIRouter(prefix="/v1/account/balance", tags=["balance"])


@router.get("/lookup")
async def lookup(discord_uuid: str):
    async with db.connection() as conn:
        balance = await balance_repo.get_balance(conn, discord_uuid)

    return {
        "discord_uuid": discord_uuid,
        "balance": balance,
        "current_balance": balance,
        "new_balance": balance,
        "user": {
            "discord_uuid": discord_uuid,
            "balance": balance,
        },
    }


@router.get("/top")
async def top(limit: int = 10):
    if limit < 1 or limit > 25:
        raise HTTPException(status_code=400, detail="limit_must_be_between_1_and_25")

    async with db.connection() as conn:
        rows = await balance_repo.list_top_balances(conn, limit=limit, positive_only=True)

    items = [
        {
            "rank": index,
            "discord_uuid": row["discord_uuid"],
            "balance": row["balance"],
            "last_updated": row["last_updated"],
        }
        for index, row in enumerate(rows, start=1)
    ]

    return {
        "items": items,
        "limit": limit,
        "count": len(items),
    }
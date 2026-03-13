from fastapi import APIRouter, HTTPException
from utcon import db

router = APIRouter()


@router.get("/account/balance/lookup")
async def lookup_balance(discord_uuid: str):

    async with db.connection() as conn:

        row = await conn.fetchrow(
            "SELECT balance FROM balances WHERE discord_uuid=$1",
            discord_uuid
        )

        if row is None:
            raise HTTPException(status_code=404, detail="Account not found")

        return {
            "discord_uuid": discord_uuid,
            "balance": float(row["balance"])
        }
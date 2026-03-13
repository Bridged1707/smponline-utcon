from fastapi import APIRouter

from utcon import db
from utcon.repositories import balance as balance_repo

router = APIRouter(prefix="/v1/account/balance", tags=["balance"])


@router.get("/lookup")
async def lookup(discord_uuid: str):

    async with db.connection() as conn:

        balance = await balance_repo.get_balance(conn, discord_uuid)

    return {"discord_uuid": discord_uuid, "balance": balance}
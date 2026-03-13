from fastapi import APIRouter

from utcon import db
from utcon.repositories import market_config as market_repo

router = APIRouter(prefix="/api/v1/market/families", tags=["market"])


@router.get("")
async def list_market_families():
    async with db.connection() as conn:
        families = await market_repo.list_families(conn)

    return {"items": families}
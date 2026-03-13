from fastapi import APIRouter

from utcon import db
from utcon.repositories import market_runtime_config as runtime_repo

router = APIRouter(prefix="/api/v1/market/config", tags=["market"])


@router.get("")
async def lookup_market_config():
    async with db.connection() as conn:
        config = await runtime_repo.get_market_config(conn)

    return config
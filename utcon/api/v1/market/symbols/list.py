from fastapi import APIRouter

from utcon import db
from utcon.repositories import market_config as market_repo

router = APIRouter(prefix="/api/v1/market/symbols", tags=["market"])


@router.get("")
async def list_market_symbols():
    async with db.connection() as conn:
        symbols = await market_repo.list_symbols(conn)

    return {"items": symbols}
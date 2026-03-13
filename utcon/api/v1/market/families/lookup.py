from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import market_config as market_repo

router = APIRouter(prefix="/api/v1/market/families", tags=["market"])


@router.get("/{family_code}")
async def lookup_market_family(family_code: str):
    async with db.connection() as conn:
        family = await market_repo.get_family(conn, family_code)
        if not family:
            raise HTTPException(status_code=404, detail="family_not_found")

        items = await market_repo.get_family_items(conn, family_code)

    return {
        "family": family,
        "items": items,
    }
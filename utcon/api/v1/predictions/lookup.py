from fastapi import APIRouter, HTTPException
from utcon import db
from utcon.repositories import predictions as repo

router = APIRouter()


@router.get("/api/v1/predictions/{market_code}")
async def get_prediction(market_code: str):
    async with db.connection() as conn:
        try:
            payload = await repo.build_market_payload(conn, market_code.upper())
        except LookupError:
            raise HTTPException(status_code=404, detail="market_not_found")

    return payload
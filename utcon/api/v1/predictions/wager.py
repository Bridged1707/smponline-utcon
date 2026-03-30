from fastapi import APIRouter, HTTPException
from utcon import db
from utcon.schemas.predictions import PredictionWagerRequest
from utcon.repositories import predictions as repo

router = APIRouter()


@router.post("/api/v1/predictions/wager")
async def wager(req: PredictionWagerRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                await repo.place_wager(conn, req)
            except LookupError as e:
                raise HTTPException(status_code=404, detail=str(e))

    return {"status": "ok"}
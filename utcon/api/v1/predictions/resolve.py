from fastapi import APIRouter, HTTPException
from utcon import db
from utcon.schemas.predictions import PredictionResolveRequest
from utcon.repositories import predictions as repo

router = APIRouter()


@router.post("/api/v1/predictions/resolve")
async def resolve(req: PredictionResolveRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                await repo.resolve_market(conn, req.market_code, req.option_code)
            except LookupError as e:
                raise HTTPException(status_code=404, detail=str(e))

    return {"status": "ok"}
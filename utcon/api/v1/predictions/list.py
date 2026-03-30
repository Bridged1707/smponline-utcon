from fastapi import APIRouter
from utcon import db
from utcon.repositories import predictions as repo

router = APIRouter()


@router.get("/api/v1/predictions")
async def list_predictions():
    async with db.connection() as conn:
        items = await repo.list_markets(conn)

    return {"items": items}
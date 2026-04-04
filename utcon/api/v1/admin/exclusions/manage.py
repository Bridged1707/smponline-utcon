from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from utcon.db import get_pool

router = APIRouter()

class ExclusionRequest(BaseModel):
    shop_id: int | None = None
    owner_name: str | None = None
    action: str  # "add" or "remove"


@router.post("/admin/exclusions/manage")
async def manage_exclusions(req: ExclusionRequest):
    if not req.shop_id and not req.owner_name:
        raise HTTPException(status_code=400, detail="Provide shop_id or owner_name")

    if req.action not in ("add", "remove"):
        raise HTTPException(status_code=400, detail="Invalid action")

    pool = await get_pool()

    async with pool.acquire() as conn:
        if req.shop_id:
            if req.action == "add":
                await conn.execute(
                    """
                    INSERT INTO shop_exclusions (shop_id)
                    VALUES ($1)
                    ON CONFLICT DO NOTHING
                    """,
                    req.shop_id,
                )
            else:
                await conn.execute(
                    "DELETE FROM shop_exclusions WHERE shop_id = $1",
                    req.shop_id,
                )

        if req.owner_name:
            if req.action == "add":
                await conn.execute(
                    """
                    INSERT INTO player_exclusions (owner_name)
                    VALUES ($1)
                    ON CONFLICT DO NOTHING
                    """,
                    req.owner_name,
                )
            else:
                await conn.execute(
                    "DELETE FROM player_exclusions WHERE owner_name = $1",
                    req.owner_name,
                )

    return {"status": "ok"}
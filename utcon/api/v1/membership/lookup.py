from __future__ import annotations

from fastapi import APIRouter, Query

from utcon import db
from utcon.repositories import membership as membership_repo

router = APIRouter(prefix="/api/v1/membership", tags=["membership"])


@router.get("/lookup")
async def lookup_membership(discord_uuid: str = Query(...)):
    async with db.connection() as conn:
        data = await membership_repo.get_effective_membership(conn, discord_uuid)
    return data
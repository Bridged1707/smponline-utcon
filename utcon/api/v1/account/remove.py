from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import account as account_repo
from utcon.schemas.account import AccountRemoveRequest

router = APIRouter(prefix="/v1/account", tags=["account"])


@router.post("/remove")
async def remove_account(req: AccountRemoveRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            removed = await account_repo.delete_account(conn, req.discord_uuid)

    if not removed:
        raise HTTPException(status_code=404, detail="account_not_found")

    return {
        "status": "account_removed",
        "discord_uuid": req.discord_uuid,
    }
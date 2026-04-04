from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import account as account_repo
from utcon.schemas.account import AccountAddRequest

router = APIRouter(prefix="/v1/account", tags=["account"])


@router.post("/add")
async def add_account(req: AccountAddRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            await account_repo.create_account(
                conn,
                discord_uuid=req.discord_uuid,
                mc_uuid=req.mc_uuid,
                mc_name=req.mc_name,
            )

            account = await account_repo.get_account_by_discord_uuid(conn, req.discord_uuid)

    if not account:
        raise HTTPException(status_code=500, detail="failed to create account")

    return {
        "status": "account_added",
        "account": {
            "discord_uuid": account.get("discord_uuid"),
            "mc_uuid": account.get("mc_uuid"),
            "mc_name": account.get("mc_name"),
            "verified_at": account.get("verified_at"),
        },
    }
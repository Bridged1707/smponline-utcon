from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import account as account_repo

router = APIRouter(prefix="/v1/account", tags=["account"])


@router.get("/lookup")
async def lookup_account(discord_uuid: str):
    async with db.connection() as conn:
        account = await account_repo.get_account_by_discord_uuid(conn, discord_uuid)

    if not account:
        raise HTTPException(status_code=404, detail="account_not_found")

    return {
        "status": "account_found",
        "account": {
            "discord_uuid": account.get("discord_uuid"),
            "mc_uuid": account.get("mc_uuid"),
            "mc_name": account.get("mc_name"),
            "verified_at": account.get("verified_at"),
            "created_at": account.get("created_at"),
            "roles": account.get("roles"),
            "rates": account.get("rates"),
        },
    }

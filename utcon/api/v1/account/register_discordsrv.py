from __future__ import annotations

import os

from fastapi import APIRouter, Header, HTTPException

from utcon import db
from utcon.repositories import account as account_repo
from utcon.schemas.account import DiscordSRVRegisterRequest

router = APIRouter(prefix="/v1/account/register/discordsrv", tags=["account"])


def _verify_bearer_token(authorization: str | None) -> None:
    expected = os.getenv("DISCORDSRV_REGISTER_BEARER_TOKEN", "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="discordsrv_bearer_token_not_configured")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing_bearer_token")

    provided = authorization.removeprefix("Bearer ").strip()
    if provided != expected:
        raise HTTPException(status_code=401, detail="invalid_bearer_token")


@router.post("")
async def register_from_discordsrv(
    req: DiscordSRVRegisterRequest,
    authorization: str | None = Header(default=None),
):
    _verify_bearer_token(authorization)

    async with db.connection() as conn:
        async with conn.transaction():
            account = await account_repo.upsert_account_from_discordsrv(
                conn,
                discord_uuid=req.discord_uuid,
                mc_uuid=req.mc_uuid,
                mc_name=req.mc_name,
            )
            await account_repo.resolve_pending_registration_for_discordsrv(
                conn,
                discord_uuid=req.discord_uuid,
                mc_uuid=req.mc_uuid,
                mc_name=req.mc_name,
            )

    if not account:
        raise HTTPException(status_code=500, detail="failed_to_register_account")

    return {
        "status": "registered",
        "source": req.source,
        "account": {
            "discord_uuid": account.get("discord_uuid"),
            "mc_uuid": account.get("mc_uuid"),
            "mc_name": account.get("mc_name"),
            "verified_at": account.get("verified_at"),
        },
    }


@router.post("/unlink")
async def unregister_from_discordsrv(
    req: DiscordSRVRegisterRequest,
    authorization: str | None = Header(default=None),
):
    _verify_bearer_token(authorization)

    async with db.connection() as conn:
        async with conn.transaction():
            account = await account_repo.clear_account_link_from_discordsrv(
                conn,
                discord_uuid=req.discord_uuid,
            )

    if not account:
        raise HTTPException(status_code=404, detail="account_not_found")

    return {
        "status": "unlinked",
        "source": req.source,
        "account": {
            "discord_uuid": account.get("discord_uuid"),
            "mc_uuid": account.get("mc_uuid"),
            "mc_name": account.get("mc_name"),
            "verified_at": account.get("verified_at"),
        },
    }


@router.get("/health")
async def discordsrv_registration_health(authorization: str | None = Header(default=None)):
    _verify_bearer_token(authorization)
    return {"status": "ok"}
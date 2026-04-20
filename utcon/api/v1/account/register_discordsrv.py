# utcon/api/v1/account/register/discordsrv.py

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from pydantic import BaseModel, Field

router = APIRouter(tags=["account-registration"])


class DiscordSRVRegisterRequest(BaseModel):
    discord_uuid: str = Field(..., min_length=1)
    mc_uuid: str = Field(..., min_length=1)
    mc_name: str = Field(..., min_length=1)
    source: str = Field(default="discordsrv-command", min_length=1)


class DiscordSRVRegisterResponse(BaseModel):
    status: Literal["matched", "already_registered", "unlinked"]
    discord_uuid: str
    mc_uuid: str
    mc_name: str
    verified_at: datetime | None = None
    source: str


def _get_expected_bearer_token() -> str:
    token = os.getenv("DISCORDSRV_REGISTER_BEARER_TOKEN", "").strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="DISCORDSRV_REGISTER_BEARER_TOKEN is not configured."
        )
    return token


def verify_bearer_token(
    authorization: str | None = Header(default=None),
) -> None:
    expected = _get_expected_bearer_token()

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token."
        )

    token = authorization.removeprefix("Bearer ").strip()
    if token != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid bearer token."
        )


@router.post(
    "/api/v1/account/register/discordsrv",
    response_model=DiscordSRVRegisterResponse,
)
async def register_from_discordsrv(
    payload: DiscordSRVRegisterRequest,
    request: Request,
    _: None = Depends(verify_bearer_token),
):
    """
    Expected app.state.db to be an asyncpg pool or connection-compatible object.
    """

    db = request.app.state.db
    verified_at = datetime.now(timezone.utc)

    async with db.acquire() as conn:
        existing = await conn.fetchrow(
            """
            SELECT discord_uuid, mc_uuid, mc_name, verified_at
            FROM accounts
            WHERE discord_uuid = $1
            """,
            payload.discord_uuid,
        )

        if (
            existing
            and existing["mc_uuid"] == payload.mc_uuid
            and existing["mc_name"] == payload.mc_name
            and existing["verified_at"] is not None
        ):
            return DiscordSRVRegisterResponse(
                status="already_registered",
                discord_uuid=payload.discord_uuid,
                mc_uuid=payload.mc_uuid,
                mc_name=payload.mc_name,
                verified_at=existing["verified_at"],
                source=payload.source,
            )

        await conn.execute(
            """
            INSERT INTO accounts (discord_uuid, mc_uuid, mc_name, verified_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (discord_uuid)
            DO UPDATE SET
                mc_uuid = EXCLUDED.mc_uuid,
                mc_name = EXCLUDED.mc_name,
                verified_at = EXCLUDED.verified_at
            """,
            payload.discord_uuid,
            payload.mc_uuid,
            payload.mc_name,
            verified_at.replace(tzinfo=None),
        )

        await conn.execute(
            """
            UPDATE account_register_queue
            SET status = 'matched',
                matched_owner_name = $2,
                matched_owner_uuid = NULL,
                resolved_at = NOW(),
                failure_reason = NULL
            WHERE discord_uuid = $1
              AND status = 'pending'
            """,
            payload.discord_uuid,
            payload.mc_name,
        )

    return DiscordSRVRegisterResponse(
        status="matched",
        discord_uuid=payload.discord_uuid,
        mc_uuid=payload.mc_uuid,
        mc_name=payload.mc_name,
        verified_at=verified_at,
        source=payload.source,
    )


@router.post(
    "/api/v1/account/register/discordsrv/unlink",
    response_model=DiscordSRVRegisterResponse,
)
async def unregister_from_discordsrv(
    payload: DiscordSRVRegisterRequest,
    request: Request,
    _: None = Depends(verify_bearer_token),
):
    db = request.app.state.db

    async with db.acquire() as conn:
        await conn.execute(
            """
            UPDATE accounts
            SET mc_uuid = NULL,
                mc_name = NULL,
                verified_at = NULL
            WHERE discord_uuid = $1
            """,
            payload.discord_uuid,
        )

    return DiscordSRVRegisterResponse(
        status="unlinked",
        discord_uuid=payload.discord_uuid,
        mc_uuid=payload.mc_uuid,
        mc_name=payload.mc_name,
        verified_at=None,
        source=payload.source,
    )


@router.get("/api/v1/account/register/discordsrv/health")
async def discordsrv_register_health(_: None = Depends(verify_bearer_token)):
    return {"ok": True}
from __future__ import annotations

import logging

import asyncpg
from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import casino as casino_repo
from utcon.schemas.casino import CasinoPfSaveRequest

router = APIRouter(prefix="/api/v1/casino", tags=["casino"])
logger = logging.getLogger(__name__)


@router.get("/pf/{discord_uuid}")
async def get_casino_pf_params(discord_uuid: str):
    try:
        async with db.connection() as conn:
            params = await casino_repo.get_pf_params(conn, discord_uuid=discord_uuid)
            if params is None:
                raise HTTPException(status_code=404, detail="casino_pf_params_not_found")
    except HTTPException:
        raise
    except asyncpg.PostgresError as exc:
        logger.exception(
            "casino_pf_get_failed discord_uuid=%s error=%s",
            discord_uuid,
            exc.__class__.__name__,
        )
        raise HTTPException(status_code=503, detail="casino_pf_storage_unavailable") from exc

    return {"status": "ok", "pf": params, "params": params}


@router.post("/pf/{discord_uuid}")
async def save_casino_pf_params(discord_uuid: str, req: CasinoPfSaveRequest):
    try:
        async with db.connection() as conn:
            async with conn.transaction():
                params = await casino_repo.upsert_pf_params(
                    conn,
                    discord_uuid=discord_uuid,
                    client_seed=req.client_seed,
                    server_seed=req.server_seed,
                    nonce=req.nonce,
                )
    except asyncpg.PostgresError as exc:
        logger.exception(
            "casino_pf_save_failed discord_uuid=%s error=%s",
            discord_uuid,
            exc.__class__.__name__,
        )
        raise HTTPException(status_code=503, detail="casino_pf_storage_unavailable") from exc

    return {"status": "ok", "pf": params, "params": params}

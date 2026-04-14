from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import casino as casino_repo
from utcon.schemas.casino import CasinoPfSaveRequest

router = APIRouter(prefix="/api/v1/casino", tags=["casino"])


@router.get("/pf/{discord_uuid}")
async def get_casino_pf_params(discord_uuid: str):
    async with db.connection() as conn:
        params = await casino_repo.get_pf_params(conn, discord_uuid=discord_uuid)
        if params is None:
            raise HTTPException(status_code=404, detail="casino_pf_params_not_found")
    return {"status": "ok", "pf": params, "params": params}


@router.post("/pf/{discord_uuid}")
async def save_casino_pf_params(discord_uuid: str, req: CasinoPfSaveRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            params = await casino_repo.upsert_pf_params(
                conn,
                discord_uuid=discord_uuid,
                client_seed=req.client_seed,
                server_seed=req.server_seed,
                nonce=req.nonce,
            )
    return {"status": "ok", "params": params}

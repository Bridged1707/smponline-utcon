from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import casino as casino_repo
from utcon.schemas.casino import CasinoAccountPanelStateRequest

router = APIRouter(prefix="/api/v1/casino", tags=["casino"])


@router.post("/state/account-panel")
async def save_casino_account_panel_message(req: CasinoAccountPanelStateRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            state = await casino_repo.save_account_panel_message(conn, message_id=req.message_id)
    return {"status": "ok", "state": state}


@router.get("/state/account-panel")
async def get_casino_account_panel_message():
    async with db.connection() as conn:
        state = await casino_repo.get_account_panel_message(conn)
        if state is None:
            raise HTTPException(status_code=404, detail="casino_account_panel_not_found")
    return {"state": state}

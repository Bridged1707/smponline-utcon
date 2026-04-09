from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import casino as casino_repo
from utcon.schemas.casino import CasinoGameSessionStartRequest, CasinoGameSessionSettleRequest

router = APIRouter(prefix="/api/v1/casino", tags=["casino"])


@router.post("/games/start")
async def start_casino_game_session(req: CasinoGameSessionStartRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await casino_repo.start_game_session(
                    conn,
                    discord_uuid=req.discord_uuid,
                    game_type=req.game_type,
                    wager_amount=req.wager_amount,
                    metadata=req.metadata or {},
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"status": "ok", **payload}


@router.post("/games/{session_id}/settle")
async def settle_casino_game_session(session_id: int, req: CasinoGameSessionSettleRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await casino_repo.settle_game_session(
                    conn,
                    session_id=session_id,
                    gross_payout_amount=req.gross_payout_amount,
                    outcome=req.outcome,
                    metadata=req.metadata or {},
                    requested_tier=req.membership_tier,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except RuntimeError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"status": "ok", **payload}

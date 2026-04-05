from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import casino as casino_repo
from utcon.schemas.casino import (
    CasinoBalanceUpdateRequest,
    CasinoFinancialTransactionAppendRequest,
    CasinoUserRegisterRequest,
)

router = APIRouter(prefix="/api/v1/casino", tags=["casino"])


@router.get("/users/{discord_uuid}")
async def get_casino_user(discord_uuid: str):
    async with db.connection() as conn:
        user = await casino_repo.get_user(conn, discord_uuid=discord_uuid)
        if user is None:
            raise HTTPException(status_code=404, detail="casino_user_not_found")
    return {"user": user}


@router.post("/users/{discord_uuid}/register")
async def register_casino_user(discord_uuid: str, req: CasinoUserRegisterRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            user = await casino_repo.register_user(
                conn,
                discord_uuid=discord_uuid,
                sender_external_id=req.sender_external_id,
            )
    return {"status": "ok", "user": user}


@router.post("/users/{discord_uuid}/balance/update")
async def update_casino_balance(discord_uuid: str, req: CasinoBalanceUpdateRequest):
    if req.amount_delta == 0:
        raise HTTPException(status_code=400, detail="amount_delta_must_not_be_zero")

    async with db.connection() as conn:
        async with conn.transaction():
            try:
                user = await casino_repo.update_user_balance(
                    conn,
                    discord_uuid=discord_uuid,
                    amount_delta=req.amount_delta,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"status": "ok", "user": user}


@router.post("/users/{discord_uuid}/financial-transactions/append")
async def append_casino_financial_transaction(
    discord_uuid: str,
    req: CasinoFinancialTransactionAppendRequest,
):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                transaction = await casino_repo.append_financial_transaction(
                    conn,
                    discord_uuid=discord_uuid,
                    transaction_type=req.type,
                    amount=req.amount,
                    net_amount=req.net_amount,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"status": "ok", "transaction": transaction}

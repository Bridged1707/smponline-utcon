from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import membership as membership_repo
from utcon.schemas.membership import MembershipPurchaseRequest

router = APIRouter(prefix="/api/v1/membership", tags=["membership"])


@router.post("/purchase")
async def purchase_membership(req: MembershipPurchaseRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                result = await membership_repo.purchase_membership(
                    conn,
                    discord_uuid=req.discord_uuid,
                    tier=req.tier,
                    weeks=req.weeks,
                    amount=req.amount,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "ok",
        "purchase": result,
    }
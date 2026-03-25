from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import membership as membership_repo
from utcon.schemas.membership import MembershipAdminGrantRequest, MembershipAdminRemoveRequest

router = APIRouter(prefix="/api/v1/membership/admin", tags=["membership"])


@router.post("/grant")
async def grant_membership(req: MembershipAdminGrantRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                result = await membership_repo.admin_grant_membership(
                    conn,
                    discord_uuid=req.discord_uuid,
                    tier=req.tier,
                    duration_days=req.duration_days,
                    reason=req.reason,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "ok",
        "membership": result,
    }


@router.post("/remove")
async def remove_membership(req: MembershipAdminRemoveRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                result = await membership_repo.admin_remove_membership(
                    conn,
                    discord_uuid=req.discord_uuid,
                    reason=req.reason,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "ok",
        "membership": result,
    }
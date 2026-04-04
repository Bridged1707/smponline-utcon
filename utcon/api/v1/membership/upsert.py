from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import membership as membership_repo
from utcon.schemas.membership import MembershipUpsertRequest

router = APIRouter(prefix="/api/v1/membership", tags=["membership"])


@router.post("/upsert")
async def upsert_membership(req: MembershipUpsertRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                row = await membership_repo.upsert_membership(
                    conn,
                    discord_uuid=req.discord_uuid,
                    tier=req.tier,
                    duration_days=req.duration_days,
                    reason=req.reason,
                    replace_active=req.replace_active,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

            effective = await membership_repo.get_effective_membership(conn, req.discord_uuid)

    return {
        "status": "ok",
        "membership": {
            "id": row["id"],
            "discord_uuid": row["discord_uuid"],
            "tier": row["tier"],
            "starts_at": row["starts_at"],
            "expires_at": row["expires_at"],
            "is_active": row["is_active"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        },
        "effective": effective,
    }
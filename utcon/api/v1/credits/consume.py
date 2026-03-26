from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import credits as credit_repo
from utcon.schemas.credits import (
    CommandCreditConsumeRequest,
    CommandCreditConsumeResponse,
)

router = APIRouter(prefix="/api/v1/credits", tags=["credits"])


@router.post("/consume", response_model=CommandCreditConsumeResponse)
async def consume_command_credit(request: CommandCreditConsumeRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            try:
                result = await credit_repo.consume_command_credits(
                    conn,
                    discord_uuid=request.discord_uuid,
                    command=request.command,
                    dry_run=request.dry_run,
                    metadata=request.metadata,
                )
            except LookupError as exc:
                detail = str(exc)

                if detail == "account_not_registered":
                    raise HTTPException(status_code=403, detail=detail) from exc

                raise HTTPException(status_code=400, detail=detail) from exc

    if not result["allowed"]:
        raise HTTPException(status_code=409, detail=result["reason"])

    return result
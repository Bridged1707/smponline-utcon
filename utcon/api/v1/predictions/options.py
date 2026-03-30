from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("/{market_code}/options")
async def list_prediction_options(market_code: str):
    async with db.connection() as conn:
        try:
            payload = await repo.build_market_payload(conn, market_code.strip().upper())
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "market": payload["market"],
        "options": payload["options"],
        "count": len(payload["options"]),
    }
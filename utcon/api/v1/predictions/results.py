from __future__ import annotations

from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from utcon import db
from utcon.repositories import predictions as prediction_repo

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


class PredictionNumericResultRequest(BaseModel):
    numeric_value: Decimal
    resolved_by: Optional[str] = None
    resolution_notes: Optional[str] = Field(default=None, max_length=4000)


@router.post("/{market_code}/results/numeric")
async def resolve_prediction_by_numeric_result(
    market_code: str,
    req: PredictionNumericResultRequest,
):
    normalized_market_code = market_code.strip().upper()

    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await prediction_repo.resolve_market_by_numeric_result(
                    conn,
                    market_code=normalized_market_code,
                    numeric_value=req.numeric_value,
                    resolved_by=req.resolved_by,
                    resolution_notes=req.resolution_notes,
                )
            except LookupError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "ok",
        **payload,
    }
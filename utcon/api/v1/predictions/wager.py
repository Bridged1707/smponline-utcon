from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import predictions as repo
from utcon.schemas.predictions import PredictionWagerRequest

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


def _value_error_to_http(detail: str, market_code: str) -> HTTPException:
    detail = (detail or "").strip()

    if detail == "market_closed":
        return HTTPException(
            status_code=400,
            detail={
                "error": "MARKET_CLOSED",
                "message": f"Betting is closed for market `{market_code}`.",
                "market_code": market_code,
            },
        )

    if detail == "option_not_found":
        return HTTPException(
            status_code=404,
            detail={
                "error": "OPTION_NOT_FOUND",
                "message": f"Option was not found on market `{market_code}`.",
                "market_code": market_code,
            },
        )

    if detail == "market_not_active":
        return HTTPException(
            status_code=400,
            detail={
                "error": "MARKET_NOT_ACTIVE",
                "message": f"Market `{market_code}` is not open for wagering.",
                "market_code": market_code,
            },
        )

    if detail == "insufficient_balance":
        return HTTPException(
            status_code=400,
            detail={
                "error": "INSUFFICIENT_BALANCE",
                "message": "You do not have enough diamonds for that wager.",
                "market_code": market_code,
            },
        )

    if detail == "balance_not_found":
        return HTTPException(
            status_code=400,
            detail={
                "error": "BALANCE_NOT_FOUND",
                "message": "Your balance record was not found. Contact an admin.",
                "market_code": market_code,
            },
        )

    if detail == "invalid_amount":
        return HTTPException(
            status_code=400,
            detail={
                "error": "INVALID_AMOUNT",
                "message": "Wager amount must be greater than 0.",
                "market_code": market_code,
            },
        )

    return HTTPException(status_code=400, detail=detail)


@router.post("/wager")
async def place_prediction_wager(req: PredictionWagerRequest):
    market_code = req.market_code.strip().upper()

    async with db.connection() as conn:
        async with conn.transaction():
            try:
                payload = await repo.place_wager(conn, req)
            except LookupError as exc:
                detail = str(exc).strip()
                if detail == "market_not_found":
                    raise HTTPException(
                        status_code=404,
                        detail={
                            "error": "MARKET_NOT_FOUND",
                            "message": f"Prediction market `{market_code}` was not found.",
                            "market_code": market_code,
                        },
                    ) from exc
                if detail == "option_not_found":
                    raise _value_error_to_http(detail, market_code) from exc
                raise HTTPException(status_code=404, detail=detail) from exc
            except ValueError as exc:
                raise _value_error_to_http(str(exc), market_code) from exc

    return {"status": "ok", **payload}

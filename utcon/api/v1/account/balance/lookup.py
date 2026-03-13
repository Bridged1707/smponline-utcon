# lookup.py
from fastapi import APIRouter, HTTPException, Query
from .schemas import LookupResponse
from ._db import get_pool
from .utils import get_account_and_rates

router = APIRouter()

@router.get("/v1/account/balance/lookup", response_model=LookupResponse)
async def lookup_balance(discord_uuid: str = Query(..., description="Discord ID (string)")):
    try:
        acc = await get_account_and_rates(discord_uuid)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {
        "discord_uuid": acc["discord_uuid"],
        "balance": acc["balance"],
        "roles": acc["roles"],
        "rates": acc["rates"]
    }
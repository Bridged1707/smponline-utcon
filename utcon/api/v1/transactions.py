from fastapi import APIRouter
from utcon.services.transaction_service import *

router = APIRouter()


@router.get("/transactions/lookup/auction")
def lookup_auction(
    item: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    start_time: int | None = None,
    end_time: int | None = None
):

    filters = locals()

    return lookup_auctions(filters)


@router.get("/transactions/lookup/shop")
def lookup_shop(
    item: str | None = None,
    shop_type: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    x: int | None = None,
    y: int | None = None,
    z: int | None = None
):

    filters = locals()

    return lookup_shop_transactions(filters)
from fastapi import APIRouter

from .lookup.auction import router as auction_lookup_router
from .lookup.shop import router as shop_lookup_router

router = APIRouter(prefix="/transactions", tags=["transactions"])

router.include_router(auction_lookup_router)
router.include_router(shop_lookup_router)
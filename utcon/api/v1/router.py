from fastapi import APIRouter

from .raw.shop.record import router as raw_shop_router
from .raw.transactions.record import router as raw_transactions_router
from .transactions.router import router as transactions_router

router = APIRouter(prefix="/v1")

router.include_router(raw_shop_router)
router.include_router(raw_transactions_router)
router.include_router(transactions_router)
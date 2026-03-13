from fastapi import APIRouter
from .transactions.record import router as transactions_router
from .shop.record import router as shop_router

router = APIRouter(prefix="/raw")

router.include_router(transactions_router, prefix="/transactions")
router.include_router(shop_router, prefix="/shop")
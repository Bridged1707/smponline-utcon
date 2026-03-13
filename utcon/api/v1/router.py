from fastapi import APIRouter

from .raw.shop.record import router as raw_shop_router
from .raw.transactions.record import router as raw_transaction_router
from .account.register import router as account_register_router

router = APIRouter(prefix="/v1")

router.include_router(raw_shop_router, prefix="/raw/shop")
router.include_router(raw_transaction_router, prefix="/raw/transactions")
router.include_router(account_register_router, prefix="/account")
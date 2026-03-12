from fastapi import APIRouter
from .transactions import router as transaction_router

router = APIRouter()

router.include_router(transaction_router)
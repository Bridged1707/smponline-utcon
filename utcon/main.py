from fastapi import FastAPI, APIRouter

from utcon import db

from utcon.api.v1.account.balance import lookup
from utcon.api.v1.account.balance.transfer import deposit, withdraw, transfer, topup

app = FastAPI()

v1_router = APIRouter(prefix="/v1")

v1_router.include_router(lookup.router)
v1_router.include_router(deposit.router)
v1_router.include_router(withdraw.router)
v1_router.include_router(transfer.router)
v1_router.include_router(topup.router)

app.include_router(v1_router)


@app.on_event("startup")
async def startup():
    await db.connect()


@app.on_event("shutdown")
async def shutdown():
    await db.close()
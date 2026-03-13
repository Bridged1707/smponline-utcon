# utcon.main or wherever you build app
from fastapi import FastAPI
from utcon.api.v1.account.balance import lookup, deposit, withdraw, transfer, topup, _db

app = FastAPI()

app.include_router(lookup.router)
app.include_router(deposit.router)
app.include_router(withdraw.router)
app.include_router(transfer.router)
app.include_router(topup.router)

@app.on_event("startup")
async def startup():
    await _db.get_pool()

@app.on_event("shutdown")
async def shutdown():
    await _db.close_pool()
    
@app.get("/")
def root():
    return {"status": "online"}

app.include_router(v1_router)
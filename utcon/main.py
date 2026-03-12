from fastapi import FastAPI
from utcon.api.v1.router import router as v1_router

app = FastAPI(
    title="UTCON API",
    version="1.0"
)

app.include_router(v1_router, prefix="/v1")
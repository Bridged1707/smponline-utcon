from fastapi import FastAPI
from utcon.api.v1.router import router as v1_router

app = FastAPI()

@app.get("/")
def root():
    return {"status": "online"}

app.include_router(v1_router)
import os
import importlib
import pkgutil
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI

from utcon import db

load_dotenv()

app = FastAPI(title="UTCON Utility Connector")

API_ROOT = Path(__file__).parent / "api"


def load_routers():

    for file in API_ROOT.rglob("*.py"):

        if file.name.startswith("_"):
            continue

        module_path = (
            "utcon."
            + file.relative_to(Path(__file__).parent)
            .with_suffix("")
            .as_posix()
            .replace("/", ".")
        )

        module = importlib.import_module(module_path)

        if hasattr(module, "router"):
            app.include_router(module.router)


load_routers()

for route in app.routes:
    print(route.path)


@app.on_event("startup")
async def startup():
    await db.connect()


@app.on_event("shutdown")
async def shutdown():
    await db.disconnect()
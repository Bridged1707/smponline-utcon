from fastapi import FastAPI
from pathlib import Path
import importlib

from utcon import db

app = FastAPI()

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


@app.on_event("startup")
async def startup():
    await db.connect()


@app.on_event("shutdown")
async def shutdown():
    await db.close()
import importlib
from pathlib import Path
#sex
from dotenv import load_dotenv
from fastapi import FastAPI

from utcon import db

load_dotenv()

app = FastAPI(title="UTCON Utility Connector")

API_ROOT = Path(__file__).parent / "api"


def load_routers() -> None:
    router_files = sorted(
        file
        for file in API_ROOT.rglob("*.py")
        if not file.name.startswith("_")
    )

    loaded = []

    for file in router_files:
        module_path = (
            "utcon."
            + file.relative_to(Path(__file__).parent)
            .with_suffix("")
            .as_posix()
            .replace("/", ".")
        )

        try:
            module = importlib.import_module(module_path)
        except Exception as exc:
            raise RuntimeError(f"failed importing router module {module_path}") from exc

        if hasattr(module, "router"):
            app.include_router(module.router)
            loaded.append(module_path)

    print("[UTCON] loaded router modules:")
    for module_path in loaded:
        print(f"[UTCON]   {module_path}")


load_routers()

print("[UTCON] registered routes:")
for route in app.routes:
    print(f"[UTCON]   {route.path}")


@app.on_event("startup")
async def startup():
    await db.connect()


@app.on_event("shutdown")
async def shutdown():
    await db.disconnect()

import importlib
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI

from utcon import db

load_dotenv()

app = FastAPI(title="UTCON Utility Connector")

API_ROOT = Path(__file__).parent / "api"


def _router_sort_key(file: Path) -> tuple[int, int, str]:
    """
    Load more specific routers first.

    FastAPI resolves matching routes in registration order.
    That means a dynamic route like /api/v1/predictions/{market_code}
    can steal requests intended for a static route like
    /api/v1/predictions/wagers if the dynamic router is included first.

    We bias toward:
    1. fewer path parameters
    2. shallower paths
    3. stable alphabetical ordering
    """
    try:
        text = file.read_text(encoding="utf-8")
    except Exception:
        text = ""

    dynamic_segments = text.count("{")
    depth = len(file.relative_to(API_ROOT).parts)
    return (dynamic_segments, depth, str(file))


def load_routers() -> None:
    router_files = sorted(
        (
            file
            for file in API_ROOT.rglob("*.py")
            if not file.name.startswith("_")
        ),
        key=_router_sort_key,
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

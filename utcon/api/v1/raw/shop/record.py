from fastapi import APIRouter
from utcon.services.shop_normalizer import normalize_shop

router = APIRouter()


@router.post("/record")
async def record_shops(shops: list[dict]):

    normalized = []

    for shop in shops:
        normalized.append(normalize_shop(shop))

    print(f"[UTCON] received {len(normalized)} shops")

    return {"received": len(normalized)}
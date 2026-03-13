from fastapi import APIRouter
from utcon.services.shop_normalizer import normalize_shop

router = APIRouter()


@router.post("/record")
async def record_shops(shops: list[dict]):

    normalized_shops = []

    for shop in shops:
        normalized = normalize_shop(shop)
        normalized_shops.append(normalized)

    print(f"[UTCON] received {len(normalized_shops)} shops")

    return {
        "received": len(normalized_shops)
    }
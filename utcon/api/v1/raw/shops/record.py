from fastapi import APIRouter
from typing import List, Dict, Any

from utcon import db

router = APIRouter(prefix="/v1/raw/shops", tags=["raw"])


@router.post("/record")
async def record_shops(shops: List[Dict[str, Any]]):
    print(f"[UTCON] received {len(shops)} shops")

    async with db.connection() as conn:
        async with conn.transaction():
            for shop in shops:
                await conn.execute(
                    """
                    INSERT INTO shops(
                        shop_id,
                        owner_name,
                        owner_uuid,
                        world,
                        x,
                        y,
                        z,
                        shop_type,
                        price,
                        remaining,
                        item_type,
                        item_name,
                        item_quantity,
                        snbt,
                        last_seen,
                        is_enabled
                    )
                    VALUES(
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
                    )
                    ON CONFLICT (shop_id)
                    DO UPDATE SET
                        owner_name = EXCLUDED.owner_name,
                        owner_uuid = EXCLUDED.owner_uuid,
                        world = EXCLUDED.world,
                        x = EXCLUDED.x,
                        y = EXCLUDED.y,
                        z = EXCLUDED.z,
                        shop_type = EXCLUDED.shop_type,
                        price = EXCLUDED.price,
                        remaining = EXCLUDED.remaining,
                        item_type = EXCLUDED.item_type,
                        item_name = EXCLUDED.item_name,
                        item_quantity = EXCLUDED.item_quantity,
                        snbt = EXCLUDED.snbt,
                        last_seen = EXCLUDED.last_seen
                    """,
                    shop["id"],
                    shop.get("owner_name"),
                    shop.get("owner_uuid"),
                    shop.get("world"),
                    shop.get("x"),
                    shop.get("y"),
                    shop.get("z"),
                    shop.get("shop_type"),
                    shop.get("price"),
                    shop.get("remaining"),
                    shop.get("item_type"),
                    shop.get("item_name"),
                    shop.get("item_quantity"),
                    shop.get("snbt"),
                    shop.get("last_seen"),
                    shop.get("is_enabled", True),
                )

    return {"status": "ok", "count": len(shops)}
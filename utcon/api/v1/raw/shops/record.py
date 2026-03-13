from fastapi import APIRouter
from utcon import db

router = APIRouter(prefix="/v1/raw", tags=["raw"])


@router.post("/shops/record")
@router.post("/shop/record")  # compatibility route
async def record_shops(payload: list):

    async with db.connection() as conn:

        async with conn.transaction():

            for shop in payload:

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
                        last_seen
                    )
                    VALUES(
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
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
                    shop["owner"]["name"] if shop.get("owner") else None,
                    shop["owner"]["uuid"] if shop.get("owner") else None,
                    shop["location"]["world"],
                    shop["location"]["x"],
                    shop["location"]["y"],
                    shop["location"]["z"],
                    shop["type"],
                    shop["price"],
                    shop["remaining"],
                    shop["item"]["type"],
                    shop["item"]["name"] if shop["item"].get("name") else None,
                    shop["item"]["quantity"],
                    shop["item"]["snbt"],
                    shop.get("lastSeen", 0)
                )

    return {"status": "shops_recorded", "count": len(payload)}
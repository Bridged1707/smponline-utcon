from fastapi import APIRouter
from typing import List, Dict, Any

from utcon import db
from utcon.repositories.exclusions import (
    load_player_exclusions,
    load_shop_exclusions,
    resolve_shop_enabled_state,
)

router = APIRouter(prefix="/v1/raw/shops", tags=["raw"])


@router.post("/record")
async def record_shops(shops: List[Dict[str, Any]]):
    print(f"[UTCON] received {len(shops)} shops")

    async with db.connection() as conn:
        async with conn.transaction():
            shop_exclusions = await load_shop_exclusions(conn, [shop.get("id") for shop in shops])
            player_exclusions = await load_player_exclusions(
                conn,
                [shop.get("owner_uuid") for shop in shops],
                [shop.get("owner_name") for shop in shops],
            )

            for shop in shops:
                resolved_state = resolve_shop_enabled_state(
                    shop,
                    shop_exclusions=shop_exclusions,
                    player_exclusions=player_exclusions,
                )

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
                        is_enabled,
                        notes
                    )
                    VALUES(
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
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
                        last_seen = EXCLUDED.last_seen,
                        is_enabled = CASE
                            WHEN EXCLUDED.is_enabled = FALSE THEN FALSE
                            ELSE shops.is_enabled
                        END,
                        notes = CASE
                            WHEN EXCLUDED.is_enabled = FALSE THEN EXCLUDED.notes
                            WHEN shops.is_enabled = FALSE THEN COALESCE(shops.notes, EXCLUDED.notes)
                            ELSE COALESCE(shops.notes, EXCLUDED.notes)
                        END
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
                    resolved_state["is_enabled"],
                    resolved_state["notes"],
                )

    return {"status": "ok", "count": len(shops)}

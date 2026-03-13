from fastapi import APIRouter, Request, HTTPException
from utcon.core.database import get_connection
import datetime

router = APIRouter()


@router.post("/record")
async def record_shops(request: Request):

    payload = await request.json()

    if not payload or "shops" not in payload:
        raise HTTPException(status_code=400, detail="INVALID_PAYLOAD")

    shops = payload["shops"]
    timestamp = payload.get("timestamp")

    conn = get_connection()
    cur = conn.cursor()

    inserted = 0

    for shop in shops:

        owner = shop.get("owner", {})
        location = shop.get("location", {})
        item = shop.get("item", {})

        cur.execute(
            """
            INSERT INTO shops
            (id, owner_uuid, owner_name, type, price, remaining,
             location_x, location_y, location_z, location_world, last_seen)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id)
            DO UPDATE SET
                price = EXCLUDED.price,
                remaining = EXCLUDED.remaining,
                last_seen = EXCLUDED.last_seen
            """,
            (
                shop["id"],
                owner.get("uuid"),
                owner.get("name"),
                shop.get("type"),
                shop.get("price"),
                shop.get("remaining"),
                location.get("x"),
                location.get("y"),
                location.get("z"),
                location.get("world"),
                datetime.datetime.fromtimestamp(timestamp/1000)
                if timestamp else None
            )
        )

        cur.execute(
            """
            INSERT INTO shop_items
            (shop_id, item_type, item_snbt, quantity)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (shop_id)
            DO UPDATE SET
                item_type = EXCLUDED.item_type,
                item_snbt = EXCLUDED.item_snbt,
                quantity = EXCLUDED.quantity
            """,
            (
                shop["id"],
                item.get("type"),
                item.get("snbt"),
                item.get("quantity")
            )
        )

        inserted += 1

    conn.commit()

    cur.close()
    conn.close()

    return {
        "status": "ok",
        "shops_processed": inserted
    }
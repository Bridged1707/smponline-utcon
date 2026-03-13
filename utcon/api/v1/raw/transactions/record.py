from fastapi import APIRouter
from utcon import db
import hashlib
import json

router = APIRouter(prefix="/v1/raw/transactions", tags=["raw"])


@router.post("/record")
async def record_transaction(event: dict):

    event_type = event["event"]
    timestamp = event["timestamp"]
    data = event["data"]

    raw_json = json.dumps(event, sort_keys=True)
    event_hash = hashlib.sha256(raw_json.encode()).hexdigest()

    async with db.connection() as conn:

        await conn.execute(
            """
            INSERT INTO transactions(
                hash,
                event,
                timestamp,
                data,
                item_type,
                item_name,
                snbt,
                quantity,
                unit_price,
                total_price,
                currency_amount,
                shop_x,
                shop_y,
                shop_z,
                shop_world,
                transaction_type
            )
            VALUES(
                $1,$2,$3,$4,
                $5,$6,$7,$8,
                $9,$10,$11,
                $12,$13,$14,$15,$16
            )
            ON CONFLICT (hash) DO NOTHING
            """,
            event_hash,
            event_type,
            timestamp,
            json.dumps(data),

            data.get("item", {}).get("type"),
            data.get("item", {}).get("name"),
            data.get("item", {}).get("snbt"),
            data.get("item", {}).get("amount") or data.get("amount"),

            data.get("itemPrice"),
            data.get("itemPrice") * data.get("totalAmount", 1)
            if data.get("itemPrice") else None,

            data.get("currencyAmount"),

            data.get("location", {}).get("x"),
            data.get("location", {}).get("y"),
            data.get("location", {}).get("z"),
            data.get("location", {}).get("world"),

            data.get("type")
        )

    return {"status": "transaction_recorded"}
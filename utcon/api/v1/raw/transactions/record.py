from fastapi import APIRouter
from typing import List, Dict, Any
from utcon import db
import hashlib
import json

router = APIRouter(prefix="/v1/raw/transactions", tags=["raw"])


@router.post("/record")
async def record_transactions(events: List[Dict[str, Any]]):
    async with db.connection() as conn:
        async with conn.transaction():
            for event in events:
                raw_json = json.dumps(event, sort_keys=True)
                event_hash = hashlib.sha256(raw_json.encode()).hexdigest()

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
                    event.get("event_type"),
                    event.get("created_at"),
                    json.dumps(event),
                    event.get("item_type"),
                    event.get("item_name"),
                    event.get("snbt"),
                    event.get("quantity"),
                    event.get("unit_price"),
                    event.get("total_price"),
                    event.get("currency_amount"),
                    event.get("shop_x"),
                    event.get("shop_y"),
                    event.get("shop_z"),
                    event.get("shop_world"),
                    event.get("transaction_type"),
                )

    return {"status": "ok", "count": len(events)}
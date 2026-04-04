from fastapi import APIRouter
from typing import List, Dict, Any, Optional
from utcon import db
from utcon.repositories.exclusions import load_player_exclusions, load_shop_exclusions
import hashlib
import json

router = APIRouter(prefix="/v1/raw/transactions", tags=["raw"])


async def _lookup_matching_shop(conn, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    shop_x = event.get("shop_x")
    shop_y = event.get("shop_y")
    shop_z = event.get("shop_z")
    shop_world = event.get("shop_world")
    item_type = event.get("item_type")
    transaction_type = event.get("transaction_type")

    if shop_x is None or shop_y is None or shop_z is None or not shop_world or not item_type:
        return None

    expected_shop_type = None
    if transaction_type == "buyFromShop":
        expected_shop_type = "SELLING"
    elif transaction_type == "sellToShop":
        expected_shop_type = "BUYING"

    sql = """
        SELECT shop_id, owner_uuid::text AS owner_uuid, owner_name, is_enabled, notes
        FROM shops
        WHERE world = $1
          AND x = $2
          AND y = $3
          AND z = $4
          AND item_type = $5
    """
    params = [shop_world, shop_x, shop_y, shop_z, item_type]

    if expected_shop_type is not None:
        sql += f" AND shop_type = ${len(params) + 1}"
        params.append(expected_shop_type)

    sql += " ORDER BY last_seen DESC, shop_id DESC LIMIT 1"

    row = await conn.fetchrow(sql, *params)
    return dict(row) if row else None


async def _resolve_transaction_enabled_state(conn, event: Dict[str, Any]) -> bool:
    matched_shop = await _lookup_matching_shop(conn, event)
    if matched_shop is None:
        return bool(event.get("is_enabled", True))

    if matched_shop.get("is_enabled") is False:
        return False

    shop_exclusions = await load_shop_exclusions(conn, [matched_shop.get("shop_id")])
    player_exclusions = await load_player_exclusions(
        conn,
        [matched_shop.get("owner_uuid")],
        [matched_shop.get("owner_name")],
    )

    if int(matched_shop["shop_id"]) in shop_exclusions:
        return False

    owner_uuid = str(matched_shop.get("owner_uuid") or "").strip()
    owner_name = str(matched_shop.get("owner_name") or "").strip().lower()
    if owner_uuid and owner_uuid in player_exclusions.get("by_uuid", {}):
        return False
    if owner_name and owner_name in player_exclusions.get("by_name", {}):
        return False

    return bool(event.get("is_enabled", True))


@router.post("/record")
async def record_transactions(events: List[Dict[str, Any]]):
    async with db.connection() as conn:
        async with conn.transaction():
            for event in events:
                raw_json = json.dumps(event, sort_keys=True)
                event_hash = hashlib.sha256(raw_json.encode()).hexdigest()
                is_enabled = await _resolve_transaction_enabled_state(conn, event)

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
                        transaction_type,
                        is_enabled
                    )
                    VALUES(
                        $1,$2,$3,$4,
                        $5,$6,$7,$8,
                        $9,$10,$11,
                        $12,$13,$14,$15,$16,$17
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
                    is_enabled,
                )

    return {"status": "ok", "count": len(events)}

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional


LOCATION_REQUIRED_TYPES = {
    "SHOP_INVENTORY_UPDATE",
    "SHOP_PRICE_UPDATE",
}
EVENT_CURSOR_INITIALIZED_TYPES = {
    "SHOP_SALE",
    "AUCTION_SALE",
}
OWNER_COMPATIBLE_ALERT_TYPES = {
    "NEW_SHOP",
    "SHOP_SALE",
    "SHOP_INVENTORY_UPDATE",
    "SHOP_PRICE_UPDATE",
}


def _row_to_dict(row) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


def _hash_snbt(snbt: Optional[str]) -> Optional[str]:
    if snbt is None:
        return None
    raw = snbt.strip()
    if not raw:
        return None
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def validate_alert_payload(payload: Dict[str, Any]) -> None:
    payload["alert_type"] = str(payload["alert_type"]).strip().upper()
    payload["target_type"] = str(payload["target_type"]).strip().upper()

    alert_type = payload["alert_type"]
    target_type = payload["target_type"]

    raw_target_key = str(payload["target_key"]).strip()
    if not raw_target_key:
        raise ValueError("target_key_required")

    if target_type == "OWNER":
        payload["target_key"] = raw_target_key
    else:
        payload["target_key"] = raw_target_key.upper()

    if target_type not in {"ITEM", "SYMBOL", "OWNER"}:
        raise ValueError("invalid_target_type")

    if alert_type in LOCATION_REQUIRED_TYPES:
        missing = [key for key in ("world", "x", "y", "z") if payload.get(key) is None]
        if missing:
            raise ValueError(f"missing_location_fields:{','.join(missing)}")

    if alert_type == "SYMBOL_PRICE":
        if target_type != "SYMBOL":
            raise ValueError("symbol_price_requires_symbol_target")
        if payload.get("min_threshold") is None and payload.get("max_threshold") is None:
            raise ValueError("symbol_price_requires_min_or_max_threshold")

    if target_type == "OWNER":
        if alert_type not in OWNER_COMPATIBLE_ALERT_TYPES:
            raise ValueError("owner_target_not_supported_for_alert_type")
        if payload.get("snbt"):
            raise ValueError("owner_target_cannot_use_snbt")

    if target_type == "SYMBOL" and alert_type != "SYMBOL_PRICE":
        raise ValueError("symbol_target_only_supported_for_symbol_price")

    if payload.get("min_threshold") is not None and payload.get("max_threshold") is not None:
        if float(payload["min_threshold"]) > float(payload["max_threshold"]):
            raise ValueError("min_threshold_must_be_lte_max_threshold")

    if payload.get("stock_minimum") is not None and payload.get("stock_maximum") is not None:
        if int(payload["stock_minimum"]) > int(payload["stock_maximum"]):
            raise ValueError("stock_minimum_must_be_lte_stock_maximum")


async def create_alert(conn, payload: Dict[str, Any]) -> Dict[str, Any]:
    validate_alert_payload(payload)

    row = await conn.fetchrow(
        """
        INSERT INTO user_alerts (
            discord_uuid,
            alert_type,
            target_type,
            target_key,
            target_name,
            snbt,
            snbt_hash,
            min_threshold,
            max_threshold,
            stock_minimum,
            stock_maximum,
            world,
            x,
            y,
            z,
            cooldown_seconds,
            notes
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
        )
        RETURNING *
        """,
        payload["discord_uuid"],
        payload["alert_type"],
        payload["target_type"],
        payload["target_key"],
        payload.get("target_name"),
        payload.get("snbt"),
        _hash_snbt(payload.get("snbt")),
        payload.get("min_threshold"),
        payload.get("max_threshold"),
        payload.get("stock_minimum"),
        payload.get("stock_maximum"),
        payload.get("world"),
        payload.get("x"),
        payload.get("y"),
        payload.get("z"),
        payload.get("cooldown_seconds", 300),
        payload.get("notes"),
    )
    alert = dict(row)

    if alert["alert_type"] in EVENT_CURSOR_INITIALIZED_TYPES:
        await conn.execute(
            """
            INSERT INTO alert_match_state (alert_id, state_key, last_event_ts, metadata)
            VALUES (
                $1,
                'cursor',
                (SELECT COALESCE(MAX(timestamp), 0) FROM transactions WHERE is_enabled = TRUE),
                $2::jsonb
            )
            ON CONFLICT (alert_id, state_key)
            DO NOTHING
            """,
            alert["id"],
            json.dumps({"initialized_live_cursor": True}),
        )

    return alert


async def list_alerts(
    conn,
    *,
    discord_uuid: Optional[str] = None,
    active_only: bool = False,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    where = []
    params: List[Any] = []

    def add(value: Any) -> str:
        params.append(value)
        return f"${len(params)}"

    if discord_uuid:
        where.append(f"discord_uuid = {add(discord_uuid)}")
    if active_only:
        where.append("is_active = TRUE")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    params.append(limit)

    rows = await conn.fetch(
        f"""
        SELECT *
        FROM user_alerts
        {where_sql}
        ORDER BY created_at DESC, id DESC
        LIMIT ${len(params)}
        """,
        *params,
    )
    return [dict(row) for row in rows]


async def get_alert(conn, alert_id: int) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow("SELECT * FROM user_alerts WHERE id = $1", alert_id)
    return _row_to_dict(row)


async def alert_belongs_to_discord_uuid(conn, alert_id: int, discord_uuid: str) -> bool:
    owner = await conn.fetchval(
        "SELECT 1 FROM user_alerts WHERE id = $1 AND discord_uuid = $2",
        alert_id,
        discord_uuid,
    )
    return owner is not None


async def set_alert_active(conn, alert_id: int, discord_uuid: str, is_active: bool) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE user_alerts
        SET is_active = $3
        WHERE id = $1 AND discord_uuid = $2
        RETURNING *
        """,
        alert_id,
        discord_uuid,
        is_active,
    )
    return _row_to_dict(row)


async def delete_alert(conn, alert_id: int, discord_uuid: str) -> bool:
    status = await conn.execute(
        "DELETE FROM user_alerts WHERE id = $1 AND discord_uuid = $2",
        alert_id,
        discord_uuid,
    )
    return status.endswith("1")


async def get_state(conn, alert_id: int, state_key: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT *
        FROM alert_match_state
        WHERE alert_id = $1 AND state_key = $2
        """,
        alert_id,
        state_key,
    )
    return _row_to_dict(row)


async def list_states_for_alert(conn, alert_id: int) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM alert_match_state
        WHERE alert_id = $1
        ORDER BY id ASC
        """,
        alert_id,
    )
    return [dict(row) for row in rows]


async def upsert_state(conn, payload: Dict[str, Any]) -> Dict[str, Any]:
    metadata_json = json.dumps(payload.get("metadata", {}))

    row = await conn.fetchrow(
        """
        INSERT INTO alert_match_state (
            alert_id,
            state_key,
            last_seen_ts,
            last_event_ts,
            last_seen_price,
            last_seen_remaining,
            last_in_band,
            metadata
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
        ON CONFLICT (alert_id, state_key)
        DO UPDATE SET
            last_seen_ts = EXCLUDED.last_seen_ts,
            last_event_ts = EXCLUDED.last_event_ts,
            last_seen_price = EXCLUDED.last_seen_price,
            last_seen_remaining = EXCLUDED.last_seen_remaining,
            last_in_band = EXCLUDED.last_in_band,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING *
        """,
        payload["alert_id"],
        payload["state_key"],
        payload.get("last_seen_ts"),
        payload.get("last_event_ts"),
        payload.get("last_seen_price"),
        payload.get("last_seen_remaining"),
        payload.get("last_in_band"),
        metadata_json,
    )
    return dict(row)


async def create_event(conn, payload: Dict[str, Any]) -> Dict[str, Any]:
    metadata_json = json.dumps(payload.get("metadata", {}))
    row = await conn.fetchrow(
        """
        INSERT INTO alert_events (
            alert_id,
            discord_uuid,
            event_type,
            title,
            body,
            source_key,
            dedupe_key,
            metadata
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
            metadata = EXCLUDED.metadata
        RETURNING *
        """,
        payload["alert_id"],
        payload["discord_uuid"],
        payload["event_type"],
        payload["title"],
        payload["body"],
        payload.get("source_key"),
        payload["dedupe_key"],
        metadata_json,
    )
    return dict(row)


async def list_pending_events(conn, *, limit: int = 50) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM alert_events
        WHERE delivery_status = 'pending'
        ORDER BY triggered_at ASC, id ASC
        LIMIT $1
        """,
        limit,
    )
    return [dict(row) for row in rows]


async def mark_event_delivered(conn, event_id: int) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE alert_events
        SET delivery_status = 'delivered',
            delivered_at = NOW()
        WHERE id = $1
        RETURNING *
        """,
        event_id,
    )
    return _row_to_dict(row)


async def mark_event_failed(conn, event_id: int, error: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE alert_events
        SET delivery_status = 'failed',
            delivery_attempts = delivery_attempts + 1,
            last_delivery_error = $2
        WHERE id = $1
        RETURNING *
        """,
        event_id,
        error,
    )
    return _row_to_dict(row)

async def update_alert(conn, alert_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    current = await get_alert(conn, alert_id)
    if current is None:
        return None

    merged = {**current, **{k: v for k, v in payload.items() if k != "discord_uuid" and v is not None}}
    if payload.get("target_key") is not None:
        raw_target_key = str(payload["target_key"]).strip()
        if not raw_target_key:
            raise ValueError("target_key_required")
        merged["target_key"] = raw_target_key if str(current.get("target_type") or "").upper() == "OWNER" else raw_target_key.upper()

    if payload.get("notes") is None and "notes" in payload:
        merged["notes"] = None

    validate_alert_payload(merged)

    row = await conn.fetchrow(
        """
        UPDATE user_alerts
        SET target_key = $3,
            target_name = $4,
            min_threshold = $5,
            max_threshold = $6,
            stock_minimum = $7,
            stock_maximum = $8,
            cooldown_seconds = $9,
            notes = $10,
            updated_at = NOW()
        WHERE id = $1 AND discord_uuid = $2
        RETURNING *
        """,
        alert_id,
        payload["discord_uuid"],
        merged.get("target_key"),
        merged.get("target_name"),
        merged.get("min_threshold"),
        merged.get("max_threshold"),
        merged.get("stock_minimum"),
        merged.get("stock_maximum"),
        merged.get("cooldown_seconds", 300),
        merged.get("notes"),
    )
    return _row_to_dict(row)

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional


def _clean_owner_name(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    return raw.lower() if raw else None


async def load_shop_exclusions(conn, shop_ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
    normalized_ids = sorted({int(shop_id) for shop_id in shop_ids if shop_id is not None})
    if not normalized_ids:
        return {}

    rows = await conn.fetch(
        """
        SELECT shop_id, reason, is_active
        FROM shop_exclusions
        WHERE is_active = TRUE
          AND shop_id = ANY($1::bigint[])
        """,
        normalized_ids,
    )
    return {int(row["shop_id"]): dict(row) for row in rows}


async def load_player_exclusions(
    conn,
    owner_uuids: Iterable[str],
    owner_names: Iterable[str],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    normalized_uuids = sorted({str(owner_uuid) for owner_uuid in owner_uuids if owner_uuid})
    normalized_names = sorted({_clean_owner_name(owner_name) for owner_name in owner_names if _clean_owner_name(owner_name)})

    by_uuid: Dict[str, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}

    if normalized_uuids:
        rows = await conn.fetch(
            """
            SELECT owner_uuid::text AS owner_uuid, owner_name, reason, is_active
            FROM player_exclusions
            WHERE is_active = TRUE
              AND owner_uuid IS NOT NULL
              AND owner_uuid = ANY($1::uuid[])
            """,
            normalized_uuids,
        )
        by_uuid = {str(row["owner_uuid"]): dict(row) for row in rows}

    if normalized_names:
        rows = await conn.fetch(
            """
            SELECT owner_uuid::text AS owner_uuid, owner_name, reason, is_active
            FROM player_exclusions
            WHERE is_active = TRUE
              AND owner_name IS NOT NULL
              AND LOWER(owner_name) = ANY($1::text[])
            """,
            normalized_names,
        )
        by_name = {_clean_owner_name(row["owner_name"]): dict(row) for row in rows if _clean_owner_name(row["owner_name"])}

    return {
        "by_uuid": by_uuid,
        "by_name": by_name,
    }


def resolve_shop_enabled_state(
    shop: Dict[str, Any],
    *,
    shop_exclusions: Dict[int, Dict[str, Any]],
    player_exclusions: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    shop_id = int(shop["id"])
    owner_uuid = str(shop.get("owner_uuid") or "").strip()
    owner_name = _clean_owner_name(shop.get("owner_name"))

    shop_rule = shop_exclusions.get(shop_id)
    if shop_rule is not None:
        reason = str(shop_rule.get("reason") or "shop exclusion list").strip()
        return {
            "is_enabled": False,
            "notes": f"Auto-disabled: shop {shop_id} is on the exclusion list ({reason}).",
            "exclusion_source": "shop",
        }

    player_rule = None
    if owner_uuid:
        player_rule = player_exclusions.get("by_uuid", {}).get(owner_uuid)
    if player_rule is None and owner_name:
        player_rule = player_exclusions.get("by_name", {}).get(owner_name)
    if player_rule is not None:
        reason = str(player_rule.get("reason") or "player exclusion list").strip()
        target = str(player_rule.get("owner_name") or owner_uuid or owner_name or "owner").strip()
        return {
            "is_enabled": False,
            "notes": f"Auto-disabled: owner {target} is on the player exclusion list ({reason}).",
            "exclusion_source": "player",
        }

    return {
        "is_enabled": True,
        "notes": "Enabled by default.",
        "exclusion_source": None,
    }

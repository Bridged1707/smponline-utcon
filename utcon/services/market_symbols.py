from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from utcon.repositories import market_config as market_repo


@dataclass(slots=True)
class ResolvedSymbolQuery:
    symbol: dict[str, Any]
    matched_by: str
    matched_value: str


def normalize_lookup_token(value: str | None) -> str:
    if not value:
        return ""
    cleaned = []
    for ch in value.upper().replace("_", " ").replace("-", " "):
        if ch.isalnum():
            cleaned.append(ch)
        elif ch.isspace():
            cleaned.append(" ")
    return " ".join("".join(cleaned).split())


async def build_symbol_composition(conn, symbol_code: str) -> dict[str, Any] | None:
    symbol = await market_repo.get_symbol(conn, symbol_code)
    if not symbol:
        return None

    symbol_families = await market_repo.get_symbol_families(conn, symbol_code)
    form_rules = await market_repo.get_symbol_form_rules(conn, symbol_code)
    item_overrides = await market_repo.get_symbol_item_overrides(conn, symbol_code)

    family_items: dict[str, list[dict[str, Any]]] = {}
    for family in symbol_families:
        family_items[family["family_code"]] = await market_repo.get_family_items(conn, family["family_code"])

    include_map: dict[str, dict[str, Any]] = {}
    for family in symbol_families:
        include_map[family["family_code"]] = {
            "include_all_forms": family["include_all_forms"],
            "rules": {},
        }

    for rule in form_rules:
        include_map.setdefault(
            rule["family_code"],
            {"include_all_forms": False, "rules": {}},
        )["rules"][rule["form_kind"]] = rule["include"]

    overrides_by_item_id = {row["family_item_id"]: row for row in item_overrides}
    grouped_families: dict[str, list[dict[str, Any]]] = defaultdict(list)
    resolved_items: list[dict[str, Any]] = []

    for family in symbol_families:
        family_code = family["family_code"]
        family_include_config = include_map.get(
            family_code,
            {"include_all_forms": False, "rules": {}},
        )

        for item in family_items.get(family_code, []):
            if not item["is_active"]:
                continue

            override = overrides_by_item_id.get(item["id"])
            if override is not None:
                include_item = override["include"]
                quantity_multiplier = (
                    override["override_multiplier"]
                    if override["override_multiplier"] is not None
                    else item["quantity_multiplier"]
                )
            else:
                if family_include_config["include_all_forms"]:
                    include_item = True
                else:
                    include_item = family_include_config["rules"].get(item["form_kind"], False)
                quantity_multiplier = item["quantity_multiplier"]

            if not include_item:
                continue

            resolved_item = {
                "family_item_id": item["id"],
                "family_code": family_code,
                "family_name": family["family_name"],
                "base_unit_name": family["base_unit_name"],
                "item_type": item["item_type"],
                "item_name": item["item_name"],
                "snbt": item["snbt"],
                "snbt_hash": item["snbt_hash"],
                "form_kind": item["form_kind"],
                "quantity_multiplier": quantity_multiplier,
                "sort_order": item["sort_order"],
                "is_override": override is not None,
            }
            resolved_items.append(resolved_item)
            grouped_families[family_code].append(resolved_item)

    resolved_items.sort(
        key=lambda item: (
            item["family_code"],
            item["sort_order"],
            item["item_type"],
            item["family_item_id"],
        )
    )

    families_payload = []
    for family in symbol_families:
        families_payload.append(
            {
                "family_code": family["family_code"],
                "family_name": family["family_name"],
                "base_unit_name": family["base_unit_name"],
                "include_all_forms": family["include_all_forms"],
                "items": sorted(
                    grouped_families.get(family["family_code"], []),
                    key=lambda item: (
                        item["sort_order"],
                        item["item_type"],
                        item["family_item_id"],
                    ),
                ),
            }
        )

    return {
        "symbol": symbol,
        "families": families_payload,
        "items": resolved_items,
    }


async def resolve_symbol_from_query(conn, raw_query: str) -> ResolvedSymbolQuery | None:
    query = raw_query.strip()
    if not query:
        return None

    symbol_code = query.upper()
    symbol = await market_repo.get_symbol(conn, symbol_code)
    if symbol and symbol.get("is_active", True):
        return ResolvedSymbolQuery(symbol=symbol, matched_by="symbol_code", matched_value=symbol_code)

    normalized_query = normalize_lookup_token(query)
    if not normalized_query:
        return None

    symbols = await market_repo.list_symbols(conn)
    active_symbols = [row for row in symbols if row.get("is_active", True)]

    for candidate in active_symbols:
        if normalize_lookup_token(candidate.get("name")) == normalized_query:
            return ResolvedSymbolQuery(
                symbol=candidate,
                matched_by="symbol_name",
                matched_value=candidate.get("name") or candidate["code"],
            )

    for candidate in active_symbols:
        composition = await build_symbol_composition(conn, candidate["code"])
        if not composition:
            continue
        for item in composition["items"]:
            item_name = item.get("item_name")
            item_type = item.get("item_type")
            if item_name and normalize_lookup_token(item_name) == normalized_query:
                return ResolvedSymbolQuery(
                    symbol=candidate,
                    matched_by="item_name",
                    matched_value=item_name,
                )
            if item_type and normalize_lookup_token(item_type) == normalized_query:
                return ResolvedSymbolQuery(
                    symbol=candidate,
                    matched_by="item_type",
                    matched_value=item_type,
                )

    return None

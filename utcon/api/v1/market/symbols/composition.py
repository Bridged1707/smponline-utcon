from collections import defaultdict

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import market_config as market_repo

router = APIRouter(prefix="/api/v1/market/symbols", tags=["market"])


@router.get("/{symbol_code}/composition")
async def lookup_market_symbol_composition(symbol_code: str):
    async with db.connection() as conn:
        symbol = await market_repo.get_symbol(conn, symbol_code)
        if not symbol:
            raise HTTPException(status_code=404, detail="symbol_not_found")

        symbol_families = await market_repo.get_symbol_families(conn, symbol_code)
        form_rules = await market_repo.get_symbol_form_rules(conn, symbol_code)
        item_overrides = await market_repo.get_symbol_item_overrides(conn, symbol_code)

        family_items = {}
        for family in symbol_families:
            family_items[family["family_code"]] = await market_repo.get_family_items(
                conn, family["family_code"]
            )

    include_map = {}
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

    resolved_items = []
    grouped_families = defaultdict(list)

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
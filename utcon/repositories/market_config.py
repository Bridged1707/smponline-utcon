from typing import Any, Dict, List, Optional


async def list_families(conn) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            code,
            name,
            base_unit_name,
            description,
            is_active,
            created_at,
            updated_at
        FROM market_asset_families
        ORDER BY code
        """
    )

    return [dict(row) for row in rows]


async def get_family(conn, family_code: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT
            code,
            name,
            base_unit_name,
            description,
            is_active,
            created_at,
            updated_at
        FROM market_asset_families
        WHERE code = $1
        """,
        family_code,
    )

    return dict(row) if row else None


async def get_family_items(conn, family_code: str) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            id,
            family_code,
            item_type,
            item_name,
            snbt,
            snbt_hash,
            quantity_multiplier,
            form_kind,
            sort_order,
            is_active,
            created_at
        FROM market_asset_family_items
        WHERE family_code = $1
        ORDER BY sort_order ASC, item_type ASC, id ASC
        """,
        family_code,
    )

    return [dict(row) for row in rows]


async def list_symbols(conn) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            code,
            name,
            description,
            pricing_method,
            display_price_source,
            is_active,
            created_at,
            updated_at
        FROM market_symbols
        ORDER BY code
        """
    )

    return [dict(row) for row in rows]


async def get_symbol(conn, symbol_code: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT
            code,
            name,
            description,
            pricing_method,
            display_price_source,
            is_active,
            created_at,
            updated_at
        FROM market_symbols
        WHERE code = $1
        """,
        symbol_code,
    )

    return dict(row) if row else None


async def get_symbol_families(conn, symbol_code: str) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            msf.id,
            msf.symbol_code,
            msf.family_code,
            msf.include_all_forms,
            msf.is_active,
            msf.created_at,
            maf.name AS family_name,
            maf.base_unit_name,
            maf.description AS family_description
        FROM market_symbol_families msf
        JOIN market_asset_families maf
          ON maf.code = msf.family_code
        WHERE msf.symbol_code = $1
        ORDER BY msf.family_code ASC
        """,
        symbol_code,
    )

    return [dict(row) for row in rows]


async def get_symbol_form_rules(conn, symbol_code: str) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            id,
            symbol_code,
            family_code,
            form_kind,
            include,
            created_at
        FROM market_symbol_family_form_rules
        WHERE symbol_code = $1
        ORDER BY family_code ASC, form_kind ASC
        """,
        symbol_code,
    )

    return [dict(row) for row in rows]


async def get_symbol_item_overrides(conn, symbol_code: str) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            msio.id,
            msio.symbol_code,
            msio.family_item_id,
            msio.include,
            msio.override_multiplier,
            msio.created_at,
            mafi.family_code,
            mafi.item_type,
            mafi.item_name,
            mafi.snbt,
            mafi.snbt_hash,
            mafi.quantity_multiplier,
            mafi.form_kind,
            mafi.sort_order
        FROM market_symbol_item_overrides msio
        JOIN market_asset_family_items mafi
          ON mafi.id = msio.family_item_id
        WHERE msio.symbol_code = $1
        ORDER BY mafi.family_code ASC, mafi.sort_order ASC, mafi.item_type ASC
        """,
        symbol_code,
    )

    return [dict(row) for row in rows]
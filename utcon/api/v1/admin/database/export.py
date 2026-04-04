import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query

from utcon import db

router = APIRouter(prefix="/v1/admin/database", tags=["admin"])


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _require_admin_key(x_admin_key: Optional[str]) -> None:
    expected_key = os.getenv("UTCON_ADMIN_API_KEY")

    if not expected_key:
        raise HTTPException(
            status_code=500,
            detail="UTCON_ADMIN_API_KEY is not configured",
        )

    if x_admin_key != expected_key:
        raise HTTPException(status_code=401, detail="invalid admin key")


@router.get("/export")
async def export_database(
    confirm: bool = Query(default=False),
    schema: Optional[str] = Query(default=None),
    x_admin_key: Optional[str] = Header(default=None, alias="X-Admin-Key"),
):
    _require_admin_key(x_admin_key)

    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="set confirm=true to export the database",
        )

    async with db.connection() as conn:
        where_clause = ""
        params = []

        if schema is not None:
            where_clause = "AND table_schema = $1"
            params = [schema]

        tables = await conn.fetch(
            f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
              {where_clause}
            ORDER BY table_schema, table_name
            """,
            *params,
        )

        export_payload = {}

        for table in tables:
            table_schema = table["table_schema"]
            table_name = table["table_name"]
            qualified_name = f"{table_schema}.{table_name}"

            rows = await conn.fetch(
                f"SELECT * FROM {_quote_ident(table_schema)}.{_quote_ident(table_name)}"
            )

            export_payload[qualified_name] = {
                "count": len(rows),
                "rows": [dict(row) for row in rows],
            }

    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "table_count": len(export_payload),
        "tables": export_payload,
    }

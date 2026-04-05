from __future__ import annotations

from fastapi import APIRouter, HTTPException

from utcon import db
from utcon.repositories import casino as casino_repo
from utcon.schemas.casino import CasinoTableCreateRequest

router = APIRouter(prefix="/api/v1/casino", tags=["casino"])


@router.post("/tables")
async def create_casino_table(req: CasinoTableCreateRequest):
    async with db.connection() as conn:
        async with conn.transaction():
            table = await casino_repo.create_table(
                conn,
                channel_id=req.channel_id,
                category_id=req.category_id,
                table_number=req.table_number,
                channel_name=req.channel_name,
                category_name=req.category_name,
            )
    return {"status": "ok", "table": table}


@router.get("/tables")
async def list_casino_tables():
    async with db.connection() as conn:
        items = await casino_repo.list_tables(conn)
    return {"items": items, "count": len(items)}


@router.get("/tables/count")
async def count_casino_tables():
    async with db.connection() as conn:
        count = await casino_repo.count_tables(conn)
    return {"count": count}


@router.delete("/tables/{channel_id}")
async def delete_casino_table(channel_id: int):
    async with db.connection() as conn:
        async with conn.transaction():
            deleted = await casino_repo.delete_table(conn, channel_id=channel_id)
            if not deleted:
                raise HTTPException(status_code=404, detail="casino_table_not_found")
    return {"status": "ok", "deleted": True}


@router.delete("/tables")
async def clear_casino_tables():
    async with db.connection() as conn:
        async with conn.transaction():
            deleted_count = await casino_repo.clear_tables(conn)
    return {"status": "ok", "deleted_count": deleted_count}

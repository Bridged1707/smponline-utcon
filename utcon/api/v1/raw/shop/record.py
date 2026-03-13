from fastapi import APIRouter
from typing import List, Dict, Any
from app.core.database import get_pool
import datetime

router = APIRouter(prefix="/raw/shop", tags=["raw"])


@router.post("/record")
async def record_shops(shops: List[Dict[str, Any]]):

    pool = await get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():

            for shop in shops:

                await conn.execute("""
                INSERT INTO shops (
                    id,
                    owner_name,
                    owner_uuid,
                    price,
                    remaining,
                    type,
                    location_x,
                    location_y,
                    location_z,
                    location_world,
                    last_seen
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                ON CONFLICT (id) DO UPDATE SET
                    price = EXCLUDED.price,
                    remaining = EXCLUDED.remaining,
                    last_seen = EXCLUDED.last_seen
                """,
                shop["id"],
                shop["owner"]["name"],
                shop["owner"]["uuid"],
                shop["price"],
                shop["remaining"],
                shop["type"],
                shop["location"]["x"],
                shop["location"]["y"],
                shop["location"]["z"],
                shop["location"]["world"],
                datetime.datetime.utcnow()
                )

    return {"status": "ok"}
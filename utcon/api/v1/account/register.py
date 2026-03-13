from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.db import get_db

router = APIRouter()


@router.post("/v1/account/register")
async def register_account(account: dict, db: AsyncSession = Depends(get_db)):

    query = text("""
        INSERT INTO accounts (discord_uuid, mc_uuid, mc_name)
        VALUES (:discord_uuid, :mc_uuid, :mc_name)
        ON CONFLICT (discord_uuid)
        DO UPDATE SET mc_name = EXCLUDED.mc_name
    """)

    await db.execute(query, {
        "discord_uuid": account["discord_uuid"],
        "mc_uuid": account["mc_uuid"],
        "mc_name": account["mc_name"]
    })

    await db.commit()

    return {"status": "registered"}
from fastapi import APIRouter
from utcon.core.database import get_pool
import hashlib
import json

router = APIRouter(prefix="/raw/transactions", tags=["raw"])


def make_hash(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


@router.post("/record")
async def record_transaction(payload: dict):

    pool = await get_pool()

    tx_hash = make_hash(payload)

    async with pool.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO transactions (hash, event, timestamp, data)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (hash) DO NOTHING
            """,
            tx_hash,
            payload["event"],
            payload["timestamp"],
            json.dumps(payload["data"])
        )

    return {"status": "ok"}
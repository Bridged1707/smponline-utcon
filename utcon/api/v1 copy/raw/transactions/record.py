from fastapi import APIRouter, Request, HTTPException
import hashlib
import json

from utcon.core.database import get_connection

router = APIRouter()


def generate_hash(payload):
    normalized = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(normalized.encode()).hexdigest()


@router.post("/record")
async def record_transaction(request: Request):

    payload = await request.json()

    if not payload:
        raise HTTPException(status_code=400, detail="INVALID_PAYLOAD")

    tx_hash = generate_hash(payload)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO transactions (tx_hash, event_type, timestamp, payload)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (tx_hash) DO NOTHING
        RETURNING id
        """,
        (
            tx_hash,
            payload.get("event"),
            payload.get("timestamp"),
            json.dumps(payload)
        )
    )

    result = cur.fetchone()

    conn.commit()

    cur.close()
    conn.close()

    if result:
        return {"status": "recorded"}

    return {"status": "duplicate"}
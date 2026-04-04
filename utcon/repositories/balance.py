from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Dict, Optional


async def get_balance(conn, discord_uuid):
    return await conn.fetchval(
        """
        SELECT balance
        FROM balances
        WHERE discord_uuid=$1
        """,
        discord_uuid,
    )


async def get_balance_for_update(conn, discord_uuid: str) -> Optional[Decimal]:
    return await conn.fetchval(
        """
        SELECT balance
        FROM balances
        WHERE discord_uuid=$1
        FOR UPDATE
        """,
        discord_uuid,
    )


async def list_top_balances(conn, *, limit: int = 10, positive_only: bool = True):
    if positive_only:
        return await conn.fetch(
            """
            SELECT discord_uuid, balance, last_updated
            FROM balances
            WHERE balance > 0
            ORDER BY balance DESC, discord_uuid ASC
            LIMIT $1
            """,
            limit,
        )

    return await conn.fetch(
        """
        SELECT discord_uuid, balance, last_updated
        FROM balances
        ORDER BY balance DESC, discord_uuid ASC
        LIMIT $1
        """,
        limit,
    )


async def add_balance(conn, discord_uuid, amount):
    await conn.execute(
        """
        UPDATE balances
        SET balance = balance + $1,
            last_updated = NOW()
        WHERE discord_uuid=$2
        """,
        amount,
        discord_uuid,
    )


async def subtract_balance(conn, discord_uuid, amount):
    await conn.execute(
        """
        UPDATE balances
        SET balance = balance - $1,
            last_updated = NOW()
        WHERE discord_uuid=$2
        """,
        amount,
        discord_uuid,
    )


async def insert_balance_transaction(
    conn,
    *,
    discord_uuid: str,
    kind: str,
    amount: Decimal,
    related_discord_uuid: str | None = None,
    applied_rates: Dict[str, Any] | None = None,
    metadata: Dict[str, Any] | None = None,
) -> None:
    applied_rates_json = json.dumps(applied_rates) if applied_rates is not None else None
    metadata_json = json.dumps(metadata) if metadata is not None else None

    await conn.execute(
        """
        INSERT INTO balance_transactions(
            discord_uuid,
            kind,
            amount,
            related_discord_uuid,
            applied_rates,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
        """,
        discord_uuid,
        kind,
        amount,
        related_discord_uuid,
        applied_rates_json,
        metadata_json,
    )
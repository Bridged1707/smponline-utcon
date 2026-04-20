from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Dict, List, Optional


BALANCE_NOTIFICATION_STATUS_PENDING = "pending"
BALANCE_NOTIFICATION_STATUS_SENT = "sent"
BALANCE_NOTIFICATION_STATUS_FAILED = "failed"


def _row_to_dict(row) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


async def create_balance_notification(
    conn,
    *,
    discord_uuid: str,
    amount: Decimal,
    reason: str | None = None,
    source: str | None = None,
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    row = await conn.fetchrow(
        """
        INSERT INTO balance_notifications (
            discord_uuid,
            amount,
            reason,
            source,
            metadata,
            status
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        RETURNING *
        """,
        discord_uuid,
        amount,
        reason,
        source,
        json.dumps(metadata or {}),
        BALANCE_NOTIFICATION_STATUS_PENDING,
    )
    return dict(row)


async def list_pending_balance_notifications(
    conn,
    *,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM balance_notifications
        WHERE status = $1
        ORDER BY created_at ASC, id ASC
        LIMIT $2
        """,
        BALANCE_NOTIFICATION_STATUS_PENDING,
        limit,
    )
    return [dict(row) for row in rows]


async def mark_balance_notification_sent(conn, notification_id: int) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE balance_notifications
        SET status = $2,
            sent_at = NOW(),
            last_error = NULL
        WHERE id = $1
        RETURNING *
        """,
        notification_id,
        BALANCE_NOTIFICATION_STATUS_SENT,
    )
    return _row_to_dict(row)


async def mark_balance_notification_failed(
    conn,
    notification_id: int,
    error: str,
) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE balance_notifications
        SET status = $2,
            last_error = $3
        WHERE id = $1
        RETURNING *
        """,
        notification_id,
        BALANCE_NOTIFICATION_STATUS_FAILED,
        error,
    )
    return _row_to_dict(row)
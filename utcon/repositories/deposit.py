from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from utcon.repositories import account as account_repo
from utcon.repositories import balance as balance_repo


DEPOSIT_STATUS_PENDING = "pending"
DEPOSIT_STATUS_MATCHED = "matched"
DEPOSIT_STATUS_EXPIRED = "expired"
DEPOSIT_STATUS_FAILED = "failed"
DEPOSIT_STATUS_CANCELLED = "cancelled"

DEPOSIT_DEFAULT_TTL_MINUTES = 30


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _row_to_dict(row) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


async def expire_stale_deposit_challenges(conn) -> int:
    result = await conn.execute(
        """
        UPDATE deposit_challenge_queue
        SET status = $1,
            resolved_at = NOW(),
            failure_reason = COALESCE(failure_reason, 'deposit challenge expired')
        WHERE status = $2
          AND expires_at <= NOW()
        """,
        DEPOSIT_STATUS_EXPIRED,
        DEPOSIT_STATUS_PENDING,
    )
    try:
        return int(str(result).split()[-1])
    except Exception:
        return 0


async def get_account_and_balance(conn, discord_uuid: str) -> tuple[Optional[Dict[str, Any]], Optional[Decimal]]:
    account = await account_repo.get_account_by_discord_uuid(conn, discord_uuid)
    if not account:
        return None, None
    balance = await balance_repo.get_balance(conn, discord_uuid)
    return account, balance


async def get_pending_deposit_for_discord(conn, discord_uuid: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        SELECT *
        FROM deposit_challenge_queue
        WHERE discord_uuid = $1
          AND status = $2
        ORDER BY requested_at DESC, id DESC
        LIMIT 1
        """,
        discord_uuid,
        DEPOSIT_STATUS_PENDING,
    )
    return _row_to_dict(row)


async def list_pending_deposit_queue(conn, *, limit: int = 100) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM deposit_challenge_queue
        WHERE status = $1
        ORDER BY requested_at ASC, id ASC
        LIMIT $2
        """,
        DEPOSIT_STATUS_PENDING,
        limit,
    )
    return [dict(row) for row in rows]


async def get_deposit_queue_item(conn, queue_id: int, *, for_update: bool = False) -> Optional[Dict[str, Any]]:
    lock_sql = "FOR UPDATE" if for_update else ""
    row = await conn.fetchrow(
        f"""
        SELECT *
        FROM deposit_challenge_queue
        WHERE id = $1
        {lock_sql}
        """,
        queue_id,
    )
    return _row_to_dict(row)


async def list_candidate_deposit_shops(conn) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            s.shop_id,
            s.owner_uuid,
            s.owner_name,
            s.world,
            s.x,
            s.y,
            s.z,
            s.shop_type,
            s.price,
            s.remaining,
            s.item_type,
            s.item_name,
            s.item_quantity,
            s.snbt,
            s.last_seen
        FROM deposit_shops ds
        JOIN shops s
          ON s.shop_id = ds.shop_id
        WHERE ds.is_active = TRUE
          AND s.shop_type = 'SELLING'
        ORDER BY RANDOM()
        LIMIT 250
        """
    )
    return [dict(row) for row in rows]


async def create_deposit_challenge(conn, discord_uuid: str) -> Dict[str, Any]:
    account, _balance = await get_account_and_balance(conn, discord_uuid)
    if not account:
        raise LookupError("account_not_found")

    existing = await get_pending_deposit_for_discord(conn, discord_uuid)
    if existing is not None:
        return existing

    candidates = await list_candidate_deposit_shops(conn)
    if not candidates:
        raise ValueError("no_deposit_shops_available")

    chosen = random.choice(candidates)
    expires_at = _utcnow_naive() + timedelta(minutes=DEPOSIT_DEFAULT_TTL_MINUTES)

    challenge_price = Decimal(str(chosen["price"]))
    challenge_quantity = int(chosen["item_quantity"] or 1)
    expected_total = challenge_price

    row = await conn.fetchrow(
        """
        INSERT INTO deposit_challenge_queue(
            discord_uuid,
            challenge_shop_id,
            challenge_owner_uuid,
            challenge_owner_name,
            challenge_item_type,
            challenge_item_name,
            challenge_item_quantity,
            challenge_price,
            expected_total,
            challenge_world,
            challenge_x,
            challenge_y,
            challenge_z,
            status,
            requested_at,
            expires_at
        )
        VALUES (
            $1, $2, $3::uuid, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, NOW(), $15
        )
        RETURNING *
        """,
        discord_uuid,
        chosen["shop_id"],
        str(chosen["owner_uuid"]),
        chosen["owner_name"],
        chosen["item_type"],
        chosen.get("item_name"),
        challenge_quantity,
        challenge_price,
        expected_total,
        chosen["world"],
        chosen["x"],
        chosen["y"],
        chosen["z"],
        DEPOSIT_STATUS_PENDING,
        expires_at,
    )
    return dict(row)


async def mark_deposit_status(
    conn,
    *,
    queue_id: int,
    status: str,
    failure_reason: Optional[str] = None,
    processed_by: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE deposit_challenge_queue
        SET status = $2,
            resolved_at = NOW(),
            failure_reason = $3,
            processed_by = COALESCE($4, processed_by)
        WHERE id = $1
        RETURNING *
        """,
        queue_id,
        status,
        failure_reason,
        processed_by,
    )
    return _row_to_dict(row)


async def resolve_deposit_match(
    conn,
    *,
    queue_id: int,
    matched_transaction_id: int,
    processed_by: Optional[str] = None,
) -> Dict[str, Any]:
    queue_item = await get_deposit_queue_item(conn, queue_id, for_update=True)
    if queue_item is None:
        raise LookupError("deposit_queue_item_not_found")

    if queue_item["status"] != DEPOSIT_STATUS_PENDING:
        raise ValueError("deposit_queue_item_not_pending")

    tx = await conn.fetchrow(
        """
        SELECT
            id,
            hash,
            event,
            timestamp,
            data,
            item_type,
            item_name,
            snbt,
            quantity,
            unit_price,
            total_price,
            currency_amount,
            shop_x,
            shop_y,
            shop_z,
            shop_world,
            transaction_type
        FROM transactions
        WHERE id = $1
        FOR UPDATE
        """,
        matched_transaction_id,
    )
    if tx is None:
        raise LookupError("transaction_not_found")

    txd = dict(tx)

    if txd.get("transaction_type") != "buyFromShop":
        raise ValueError("transaction_not_buy_from_shop")

    if str(txd.get("shop_world")) != str(queue_item["challenge_world"]):
        raise ValueError("transaction_world_mismatch")
    if int(txd.get("shop_x")) != int(queue_item["challenge_x"]):
        raise ValueError("transaction_x_mismatch")
    if int(txd.get("shop_y")) != int(queue_item["challenge_y"]):
        raise ValueError("transaction_y_mismatch")
    if int(txd.get("shop_z")) != int(queue_item["challenge_z"]):
        raise ValueError("transaction_z_mismatch")

    if str(txd.get("item_type")) != str(queue_item["challenge_item_type"]):
        raise ValueError("transaction_item_type_mismatch")

    tx_unit_price = Decimal(str(txd.get("unit_price") or 0))
    challenge_price = Decimal(str(queue_item["challenge_price"]))
    if tx_unit_price != challenge_price:
        raise ValueError("transaction_price_mismatch")

    tx_currency_amount = Decimal(str(txd.get("currency_amount") or 0))
    expected_total = Decimal(str(queue_item["expected_total"]))
    if tx_currency_amount < expected_total:
        raise ValueError("transaction_amount_too_small")

    duplicate = await conn.fetchrow(
        """
        SELECT id
        FROM deposit_challenge_queue
        WHERE matched_transaction_id = $1
        """,
        matched_transaction_id,
    )
    if duplicate is not None:
        raise ValueError("transaction_already_consumed")

    current_balance = await balance_repo.get_balance_for_update(conn, queue_item["discord_uuid"])
    if current_balance is None:
        raise LookupError("balance_not_found")

    credited_amount = tx_currency_amount

    await balance_repo.add_balance(conn, queue_item["discord_uuid"], credited_amount)
    await balance_repo.insert_balance_transaction(
        conn,
        discord_uuid=queue_item["discord_uuid"],
        kind="deposit",
        amount=credited_amount,
        metadata={
            "queue_id": queue_id,
            "transaction_id": matched_transaction_id,
            "shop_id": queue_item["challenge_shop_id"],
            "shop_world": queue_item["challenge_world"],
            "shop_x": queue_item["challenge_x"],
            "shop_y": queue_item["challenge_y"],
            "shop_z": queue_item["challenge_z"],
            "item_type": queue_item["challenge_item_type"],
            "challenge_price": str(queue_item["challenge_price"]),
            "expected_total": str(queue_item["expected_total"]),
            "credited_amount": str(credited_amount),
            "transaction_quantity": int(txd.get("quantity") or 0),
            "transaction_total_price": str(txd.get("total_price") or 0),
        },
    )

    row = await conn.fetchrow(
        """
        UPDATE deposit_challenge_queue
        SET status = $2,
            resolved_at = NOW(),
            matched_transaction_id = $3,
            processed_by = COALESCE($4, processed_by)
        WHERE id = $1
        RETURNING *
        """,
        queue_id,
        DEPOSIT_STATUS_MATCHED,
        matched_transaction_id,
        processed_by,
    )
    return dict(row)
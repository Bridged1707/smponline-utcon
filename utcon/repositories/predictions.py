from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from utcon.repositories import account as account_repo
from utcon.repositories import balance as balance_repo

STATUS_ACTIVE = "open"
STATUS_CLOSED = "closed"
STATUS_RESOLVED = "resolved"
STATUS_CANCELLED = "cancelled"

SIDE_YES = "YES"
SIDE_NO = "NO"
OUTCOME_CANCELLED = "CANCELLED"

ZERO = Decimal("0")
HALF = Decimal("0.5")


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _row_to_dict(row) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


def _normalize_market_row(market: Dict[str, Any]) -> Dict[str, Any]:
    market = dict(market)
    yes_pool = Decimal(str(market.get("yes_pool") or 0))
    no_pool = Decimal(str(market.get("no_pool") or 0))
    total_volume = Decimal(str(market.get("total_volume") or 0))
    if total_volume <= 0:
        price_yes = HALF
        price_no = HALF
    else:
        price_yes = yes_pool / total_volume
        price_no = no_pool / total_volume
    market["yes_pool"] = yes_pool
    market["no_pool"] = no_pool
    market["total_volume"] = total_volume
    market["price_yes"] = price_yes
    market["price_no"] = price_no
    return market


async def list_markets(
    conn,
    *,
    status: Optional[str] = None,
    include_closed: bool = False,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    where = []
    params: List[Any] = []

    def add(value: Any) -> str:
        params.append(value)
        return f"${len(params)}"

    if status:
        where.append(f"status = {add(status)}")
    elif not include_closed:
        where.append(f"status = {add(STATUS_ACTIVE)}")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    params.append(limit)
    rows = await conn.fetch(
        f"""
        SELECT
            code,
            title,
            description,
            closes_at,
            resolves_at,
            status,
            outcome,
            created_by,
            created_at,
            updated_at,
            yes_pool,
            no_pool,
            total_volume,
            price_yes,
            price_no,
            last_trade_ts,
            resolution_notes
        FROM prediction_markets
        {where_sql}
        ORDER BY
            CASE WHEN status = 'open' THEN 0 ELSE 1 END,
            closes_at ASC NULLS LAST,
            created_at DESC,
            code ASC
        LIMIT ${len(params)}
        """,
        *params,
    )
    return [_normalize_market_row(dict(row)) for row in rows]


async def get_market(conn, code: str, *, for_update: bool = False) -> Optional[Dict[str, Any]]:
    lock_sql = "FOR UPDATE" if for_update else ""
    row = await conn.fetchrow(
        f"""
        SELECT
            code,
            title,
            description,
            closes_at,
            resolves_at,
            status,
            outcome,
            created_by,
            created_at,
            updated_at,
            yes_pool,
            no_pool,
            total_volume,
            price_yes,
            price_no,
            last_trade_ts,
            resolution_notes
        FROM prediction_markets
        WHERE code = $1
        {lock_sql}
        """,
        code,
    )
    return _normalize_market_row(dict(row)) if row else None


async def create_market(conn, payload: Dict[str, Any]) -> Dict[str, Any]:
    code = payload["code"].strip().upper()
    row = await conn.fetchrow(
        """
        INSERT INTO prediction_markets(
            code,
            title,
            description,
            closes_at,
            resolves_at,
            status,
            outcome,
            created_by,
            created_at,
            updated_at,
            yes_pool,
            no_pool,
            total_volume,
            price_yes,
            price_no,
            last_trade_ts,
            resolution_notes
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, NULL, $7,
            NOW(), NOW(), 0, 0, 0, 0.5, 0.5, NULL, NULL
        )
        RETURNING *
        """,
        code,
        payload["title"].strip(),
        payload.get("description"),
        payload["closes_at"],
        payload.get("resolves_at"),
        STATUS_ACTIVE,
        payload.get("created_by"),
    )
    market = _normalize_market_row(dict(row))
    await insert_snapshot(conn, market)
    return market


async def insert_snapshot(conn, market: Dict[str, Any], *, snapshot_ts: Optional[int] = None) -> Dict[str, Any]:
    market = _normalize_market_row(market)
    snapshot_ts = snapshot_ts or _now_ms()
    row = await conn.fetchrow(
        """
        INSERT INTO prediction_market_snapshots(
            market_code,
            snapshot_ts,
            yes_pool,
            no_pool,
            total_volume,
            price_yes,
            price_no,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        RETURNING *
        """,
        market["code"],
        snapshot_ts,
        market["yes_pool"],
        market["no_pool"],
        market["total_volume"],
        market["price_yes"],
        market["price_no"],
    )
    return dict(row)


async def get_history(
    conn,
    market_code: str,
    *,
    from_ts: int = 0,
    to_ts: int = 9_999_999_999_999,
    limit: int = 500,
    newest_first: bool = False,
) -> List[Dict[str, Any]]:
    order_sql = "DESC" if newest_first else "ASC"
    rows = await conn.fetch(
        f"""
        SELECT
            id,
            market_code,
            snapshot_ts,
            yes_pool,
            no_pool,
            total_volume,
            price_yes,
            price_no,
            created_at
        FROM prediction_market_snapshots
        WHERE market_code = $1
          AND snapshot_ts >= $2
          AND snapshot_ts <= $3
        ORDER BY snapshot_ts {order_sql}, id {order_sql}
        LIMIT $4
        """,
        market_code,
        from_ts,
        to_ts,
        limit,
    )
    return [dict(row) for row in rows]


async def get_recent_wagers(conn, market_code: str, *, limit: int = 20) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            id,
            market_code,
            discord_uuid,
            side,
            amount,
            price_yes_before,
            price_yes_after,
            payout_amount,
            created_at,
            settled_at
        FROM prediction_wagers
        WHERE market_code = $1
        ORDER BY created_at DESC, id DESC
        LIMIT $2
        """,
        market_code,
        limit,
    )
    return [dict(row) for row in rows]


async def _reprice_market(conn, market_code: str, *, last_trade_ts: Optional[int] = None) -> Dict[str, Any]:
    market = await get_market(conn, market_code, for_update=True)
    if market is None:
        raise LookupError("market_not_found")

    yes_pool = Decimal(str(market.get("yes_pool") or 0))
    no_pool = Decimal(str(market.get("no_pool") or 0))
    total_volume = yes_pool + no_pool

    if total_volume <= 0:
        price_yes = HALF
        price_no = HALF
    else:
        price_yes = yes_pool / total_volume
        price_no = no_pool / total_volume

    row = await conn.fetchrow(
        """
        UPDATE prediction_markets
        SET yes_pool = $2,
            no_pool = $3,
            total_volume = $4,
            price_yes = $5,
            price_no = $6,
            last_trade_ts = COALESCE($7, last_trade_ts),
            updated_at = NOW()
        WHERE code = $1
        RETURNING *
        """,
        market_code,
        yes_pool,
        no_pool,
        total_volume,
        price_yes,
        price_no,
        last_trade_ts,
    )
    return _normalize_market_row(dict(row))


async def place_wager(conn, *, market_code: str, discord_uuid: str, side: str, amount: Decimal) -> Dict[str, Any]:
    market = await get_market(conn, market_code, for_update=True)
    if market is None:
        raise LookupError("market_not_found")

    if market["status"] != STATUS_ACTIVE:
        raise ValueError("market_not_active")

    closes_at = market.get("closes_at")
    if closes_at is not None and closes_at <= _utcnow_naive():
        raise ValueError("market_closed")

    account = await account_repo.get_account_by_discord_uuid(conn, discord_uuid)
    if account is None:
        raise LookupError("account_not_found")

    balance = await balance_repo.get_balance_for_update(conn, discord_uuid)
    if balance is None:
        raise LookupError("balance_not_found")
    if balance < amount:
        raise ValueError("insufficient_balance")

    price_yes_before = Decimal(str(market["price_yes"]))

    await balance_repo.subtract_balance(conn, discord_uuid, amount)
    await balance_repo.insert_balance_transaction(
        conn,
        discord_uuid=discord_uuid,
        kind="prediction_wager",
        amount=-amount,
        metadata={
            "market_code": market_code,
            "side": side,
        },
    )

    if side == SIDE_YES:
        await conn.execute(
            "UPDATE prediction_markets SET yes_pool = yes_pool + $2 WHERE code = $1",
            market_code,
            amount,
        )
    else:
        await conn.execute(
            "UPDATE prediction_markets SET no_pool = no_pool + $2 WHERE code = $1",
            market_code,
            amount,
        )

    updated_market = await _reprice_market(conn, market_code, last_trade_ts=_now_ms())
    price_yes_after = Decimal(str(updated_market["price_yes"]))

    wager_row = await conn.fetchrow(
        """
        INSERT INTO prediction_wagers(
            market_code,
            discord_uuid,
            side,
            amount,
            price_yes_before,
            price_yes_after,
            payout_amount,
            created_at,
            settled_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 0, NOW(), NULL)
        RETURNING *
        """,
        market_code,
        discord_uuid,
        side,
        amount,
        price_yes_before,
        price_yes_after,
    )

    snapshot = await insert_snapshot(conn, updated_market, snapshot_ts=updated_market.get("last_trade_ts") or _now_ms())
    balance_after = await balance_repo.get_balance(conn, discord_uuid)

    return {
        "market": updated_market,
        "wager": dict(wager_row),
        "snapshot": snapshot,
        "balance_after": balance_after,
    }


async def resolve_market(
    conn,
    *,
    market_code: str,
    outcome: str,
    resolved_by: Optional[str] = None,
    resolution_notes: Optional[str] = None,
) -> Dict[str, Any]:
    market = await get_market(conn, market_code, for_update=True)
    if market is None:
        raise LookupError("market_not_found")
    if market["status"] in {STATUS_RESOLVED, STATUS_CANCELLED}:
        raise ValueError("market_already_finalized")

    if outcome == OUTCOME_CANCELLED:
        return await cancel_market(conn, market_code=market_code, cancelled_by=resolved_by, reason=resolution_notes)

    winning_side = outcome
    winning_pool_key = "yes_pool" if winning_side == SIDE_YES else "no_pool"
    losing_pool_key = "no_pool" if winning_side == SIDE_YES else "yes_pool"
    winning_pool = Decimal(str(market.get(winning_pool_key) or 0))
    losing_pool = Decimal(str(market.get(losing_pool_key) or 0))

    wagers = await conn.fetch(
        """
        SELECT id, discord_uuid, side, amount
        FROM prediction_wagers
        WHERE market_code = $1
          AND settled_at IS NULL
        ORDER BY id ASC
        """,
        market_code,
    )

    for wager in wagers:
        amount = Decimal(str(wager["amount"]))
        payout = ZERO
        if wager["side"] == winning_side:
            if winning_pool > 0:
                payout = amount + (amount / winning_pool) * losing_pool
            else:
                payout = amount
            await balance_repo.get_balance_for_update(conn, wager["discord_uuid"])
            await balance_repo.add_balance(conn, wager["discord_uuid"], payout)
            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=wager["discord_uuid"],
                kind="prediction_payout",
                amount=payout,
                metadata={
                    "market_code": market_code,
                    "outcome": outcome,
                    "wager_id": int(wager["id"]),
                },
            )
        await conn.execute(
            """
            UPDATE prediction_wagers
            SET payout_amount = $2,
                settled_at = NOW()
            WHERE id = $1
            """,
            wager["id"],
            payout,
        )

    row = await conn.fetchrow(
        """
        UPDATE prediction_markets
        SET status = $2,
            outcome = $3,
            resolution_notes = $4,
            updated_at = NOW(),
            resolves_at = COALESCE(resolves_at, NOW())
        WHERE code = $1
        RETURNING *
        """,
        market_code,
        STATUS_RESOLVED,
        outcome,
        resolution_notes,
    )
    updated_market = _normalize_market_row(dict(row))
    snapshot = await insert_snapshot(conn, updated_market)
    return {
        "market": updated_market,
        "snapshot": snapshot,
        "resolved_by": resolved_by,
    }


async def cancel_market(
    conn,
    *,
    market_code: str,
    cancelled_by: Optional[str] = None,
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    market = await get_market(conn, market_code, for_update=True)
    if market is None:
        raise LookupError("market_not_found")
    if market["status"] in {STATUS_RESOLVED, STATUS_CANCELLED}:
        raise ValueError("market_already_finalized")

    wagers = await conn.fetch(
        """
        SELECT id, discord_uuid, amount
        FROM prediction_wagers
        WHERE market_code = $1
          AND settled_at IS NULL
        ORDER BY id ASC
        """,
        market_code,
    )

    for wager in wagers:
        amount = Decimal(str(wager["amount"]))
        await balance_repo.get_balance_for_update(conn, wager["discord_uuid"])
        await balance_repo.add_balance(conn, wager["discord_uuid"], amount)
        await balance_repo.insert_balance_transaction(
            conn,
            discord_uuid=wager["discord_uuid"],
            kind="prediction_refund",
            amount=amount,
            metadata={
                "market_code": market_code,
                "wager_id": int(wager["id"]),
            },
        )
        await conn.execute(
            """
            UPDATE prediction_wagers
            SET payout_amount = $2,
                settled_at = NOW()
            WHERE id = $1
            """,
            wager["id"],
            amount,
        )

    row = await conn.fetchrow(
        """
        UPDATE prediction_markets
        SET status = $2,
            outcome = $3,
            resolution_notes = $4,
            updated_at = NOW()
        WHERE code = $1
        RETURNING *
        """,
        market_code,
        STATUS_CANCELLED,
        OUTCOME_CANCELLED,
        reason,
    )
    updated_market = _normalize_market_row(dict(row))
    snapshot = await insert_snapshot(conn, updated_market)
    return {
        "market": updated_market,
        "snapshot": snapshot,
        "cancelled_by": cancelled_by,
    }

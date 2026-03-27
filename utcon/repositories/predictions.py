from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional

from utcon.repositories import account as account_repo
from utcon.repositories import balance as balance_repo
from utcon.repositories import membership as membership_repo

STATUS_ACTIVE = "open"
STATUS_CLOSED = "closed"
STATUS_RESOLVED = "resolved"
STATUS_CANCELLED = "cancelled"

SIDE_YES = "YES"
SIDE_NO = "NO"
OUTCOME_CANCELLED = "CANCELLED"

ZERO = Decimal("0")
HALF = Decimal("0.5")
BPS_DIVISOR = Decimal("10000")
DISPLAY_QUANT = Decimal("0.0001")
TIER_FEE_BPS = {
    "free": 1000,
    "pro": 700,
    "garry": 300,
}


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _row_to_dict(row) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(DISPLAY_QUANT, rounding=ROUND_DOWN)


def _normalize_market_row(market: Dict[str, Any]) -> Dict[str, Any]:
    market = dict(market)
    yes_pool = _to_decimal(market.get("yes_pool"))
    no_pool = _to_decimal(market.get("no_pool"))
    total_volume = _to_decimal(market.get("total_volume"))
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


def _normalize_wager_row(wager: Dict[str, Any]) -> Dict[str, Any]:
    wager = dict(wager)
    for key in (
        "amount",
        "price_yes_before",
        "price_yes_after",
        "payout_amount",
        "gross_payout_amount",
        "profit_amount",
        "fee_amount",
    ):
        if key in wager:
            wager[key] = _to_decimal(wager.get(key))
    return wager


def _tier_from_membership_payload(payload: Dict[str, Any] | None) -> str:
    tier = str((payload or {}).get("tier") or "free").strip().lower()
    return tier if tier in TIER_FEE_BPS else "free"


def _fee_bps_for_tier(tier: str) -> int:
    return int(TIER_FEE_BPS.get(tier, TIER_FEE_BPS["free"]))


async def list_markets(
    conn,
    *,
    status: Optional[str] = None,
    include_closed: bool = False,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    where = []
    params: List[Any] = []
    restrict_to_bettable_open_markets = False

    def add(value: Any) -> str:
        params.append(value)
        return f"${len(params)}"

    if status:
        where.append(f"status = {add(status)}")
        restrict_to_bettable_open_markets = status == STATUS_ACTIVE
    elif not include_closed:
        where.append(f"status = {add(STATUS_ACTIVE)}")
        restrict_to_bettable_open_markets = True

    if restrict_to_bettable_open_markets:
        where.append(f"(closes_at IS NULL OR closes_at > {add(_utcnow_naive())})")

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


async def list_user_wagers(
    conn,
    *,
    discord_uuid: str,
    unsettled_only: bool = True,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    where = ["w.discord_uuid = $1"]
    params: List[Any] = [discord_uuid]

    if unsettled_only:
        where.append("w.settled_at IS NULL")

    params.append(limit)
    rows = await conn.fetch(
        f"""
        SELECT
            w.id,
            w.market_code,
            w.discord_uuid,
            w.side,
            w.amount,
            w.price_yes_before,
            w.price_yes_after,
            w.payout_amount,
            w.gross_payout_amount,
            w.profit_amount,
            w.fee_amount,
            w.outcome,
            w.membership_tier_at_wager,
            w.fee_rate_bps_at_wager,
            w.created_at,
            w.settled_at,
            m.title AS market_title,
            m.status AS market_status,
            m.outcome AS market_outcome,
            m.closes_at,
            m.resolves_at
        FROM prediction_wagers w
        JOIN prediction_markets m
          ON m.code = w.market_code
        WHERE {' AND '.join(where)}
        ORDER BY
            CASE WHEN w.settled_at IS NULL THEN 0 ELSE 1 END,
            w.created_at DESC,
            w.id DESC
        LIMIT ${len(params)}
        """,
        *params,
    )
    return [_normalize_wager_row(dict(row)) for row in rows]


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
            membership_tier_at_wager,
            fee_rate_bps_at_wager,
            gross_payout_amount,
            profit_amount,
            fee_amount,
            outcome,
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
    return [_normalize_wager_row(dict(row)) for row in rows]


async def _reprice_market(conn, market_code: str, *, last_trade_ts: Optional[int] = None) -> Dict[str, Any]:
    market = await get_market(conn, market_code, for_update=True)
    if market is None:
        raise LookupError("market_not_found")

    yes_pool = _to_decimal(market.get("yes_pool"))
    no_pool = _to_decimal(market.get("no_pool"))
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


def _estimate_wager_return(*, amount: Decimal, side: str, market: Dict[str, Any], fee_bps: int) -> Dict[str, Decimal]:
    yes_pool = _to_decimal(market.get("yes_pool"))
    no_pool = _to_decimal(market.get("no_pool"))
    winning_pool = yes_pool if side == SIDE_YES else no_pool
    losing_pool = no_pool if side == SIDE_YES else yes_pool

    if winning_pool <= 0:
        profit = ZERO
    else:
        profit = (amount / winning_pool) * losing_pool

    gross = amount + profit
    fee = (profit * Decimal(fee_bps)) / BPS_DIVISOR if profit > 0 else ZERO
    net = gross - fee
    if net < amount:
        net = amount
        fee = gross - net

    return {
        "gross_payout_amount": _quantize(gross),
        "profit_amount": _quantize(profit),
        "fee_amount": _quantize(fee),
        "net_payout_amount": _quantize(net),
    }


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

    membership = await membership_repo.get_effective_membership(conn, discord_uuid)
    membership_tier = _tier_from_membership_payload(membership)
    fee_rate_bps = _fee_bps_for_tier(membership_tier)

    balance = await balance_repo.get_balance_for_update(conn, discord_uuid)
    if balance is None:
        raise LookupError("balance_not_found")
    if balance < amount:
        raise ValueError("insufficient_balance")

    price_yes_before = _to_decimal(market["price_yes"])

    await balance_repo.subtract_balance(conn, discord_uuid, amount)
    await balance_repo.insert_balance_transaction(
        conn,
        discord_uuid=discord_uuid,
        kind="prediction_wager",
        amount=-amount,
        applied_rates={
            "prediction_fee_rate_bps": fee_rate_bps,
            "prediction_fee_percent": fee_rate_bps / 100,
        },
        metadata={
            "market_code": market_code,
            "side": side,
            "membership_tier_at_wager": membership_tier,
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
    price_yes_after = _to_decimal(updated_market["price_yes"])
    projected = _estimate_wager_return(
        amount=amount,
        side=side,
        market=updated_market,
        fee_bps=fee_rate_bps,
    )

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
            membership_tier_at_wager,
            fee_rate_bps_at_wager,
            gross_payout_amount,
            profit_amount,
            fee_amount,
            outcome,
            created_at,
            settled_at,
            notification_attempts,
            notified_at,
            notification_last_error
        )
        VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, 0, 0, 0, NULL, NOW(), NULL, 0, NULL, NULL)
        RETURNING *
        """,
        market_code,
        discord_uuid,
        side,
        amount,
        price_yes_before,
        price_yes_after,
        membership_tier,
        fee_rate_bps,
    )

    snapshot = await insert_snapshot(conn, updated_market, snapshot_ts=updated_market.get("last_trade_ts") or _now_ms())
    balance_after = await balance_repo.get_balance(conn, discord_uuid)

    return {
        "market": updated_market,
        "wager": _normalize_wager_row(dict(wager_row)),
        "snapshot": snapshot,
        "balance_after": balance_after,
        "membership_tier_at_wager": membership_tier,
        "fee_rate_bps_at_wager": fee_rate_bps,
        "projected_return_if_resolved_now": projected,
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

    closes_at = market.get("closes_at")
    if closes_at is not None and closes_at > _utcnow_naive():
        raise ValueError("market_still_open_for_betting")

    winning_side = outcome
    winning_pool_key = "yes_pool" if winning_side == SIDE_YES else "no_pool"
    losing_pool_key = "no_pool" if winning_side == SIDE_YES else "yes_pool"
    winning_pool = _to_decimal(market.get(winning_pool_key))
    losing_pool = _to_decimal(market.get(losing_pool_key))

    wagers = await conn.fetch(
        """
        SELECT id, discord_uuid, side, amount, membership_tier_at_wager, fee_rate_bps_at_wager
        FROM prediction_wagers
        WHERE market_code = $1
          AND settled_at IS NULL
        ORDER BY id ASC
        """,
        market_code,
    )

    settlement_rows: List[Dict[str, Any]] = []

    for wager in wagers:
        amount = _to_decimal(wager["amount"])
        payout = ZERO
        gross_payout = ZERO
        profit = ZERO
        fee = ZERO
        wager_outcome = "LOSS"
        fee_bps = int(wager.get("fee_rate_bps_at_wager") or 0)
        membership_tier = str(wager.get("membership_tier_at_wager") or "free").lower()

        if wager["side"] == winning_side:
            wager_outcome = "WIN"
            if winning_pool > 0:
                profit = (amount / winning_pool) * losing_pool
            gross_payout = amount + profit
            fee = (profit * Decimal(fee_bps)) / BPS_DIVISOR if profit > 0 else ZERO
            payout = gross_payout - fee
            if payout < amount:
                payout = amount
                fee = gross_payout - payout

            payout = _quantize(payout)
            gross_payout = _quantize(gross_payout)
            profit = _quantize(profit)
            fee = _quantize(fee)

            await balance_repo.get_balance_for_update(conn, wager["discord_uuid"])
            await balance_repo.add_balance(conn, wager["discord_uuid"], payout)
            await balance_repo.insert_balance_transaction(
                conn,
                discord_uuid=wager["discord_uuid"],
                kind="prediction_payout",
                amount=payout,
                applied_rates={
                    "prediction_fee_rate_bps": fee_bps,
                    "prediction_fee_percent": fee_bps / 100,
                },
                metadata={
                    "market_code": market_code,
                    "outcome": outcome,
                    "wager_id": int(wager["id"]),
                    "result": wager_outcome,
                    "membership_tier_at_wager": membership_tier,
                    "gross_payout_amount": str(gross_payout),
                    "profit_amount": str(profit),
                    "fee_amount": str(fee),
                    "net_payout_amount": str(payout),
                },
            )
        else:
            payout = ZERO
            gross_payout = ZERO
            profit = ZERO
            fee = ZERO

        await conn.execute(
            """
            UPDATE prediction_wagers
            SET payout_amount = $2,
                gross_payout_amount = $3,
                profit_amount = $4,
                fee_amount = $5,
                outcome = $6,
                settled_at = NOW(),
                notification_attempts = 0,
                notified_at = NULL,
                notification_last_error = NULL
            WHERE id = $1
            """,
            wager["id"],
            payout,
            gross_payout,
            profit,
            fee,
            wager_outcome,
        )

        settlement_rows.append(
            {
                "wager_id": int(wager["id"]),
                "discord_uuid": str(wager["discord_uuid"]),
                "side": str(wager["side"]),
                "amount": amount,
                "outcome": wager_outcome,
                "membership_tier_at_wager": membership_tier,
                "fee_rate_bps_at_wager": fee_bps,
                "gross_payout_amount": gross_payout,
                "profit_amount": profit,
                "fee_amount": fee,
                "net_payout_amount": payout,
            }
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
        "settlements": settlement_rows,
    }


async def close_market(
    conn,
    *,
    market_code: str,
    closed_by: Optional[str] = None,
    close_reason: Optional[str] = None,
) -> Dict[str, Any]:
    market = await get_market(conn, market_code, for_update=True)
    if market is None:
        raise LookupError("market_not_found")
    if market["status"] == STATUS_RESOLVED:
        raise ValueError("market_already_resolved")
    if market["status"] == STATUS_CANCELLED:
        raise ValueError("market_already_cancelled")
    if market["status"] == STATUS_CLOSED:
        raise ValueError("market_already_closed")

    row = await conn.fetchrow(
        """
        UPDATE prediction_markets
        SET status = $2,
            updated_at = NOW(),
            resolution_notes = CASE
                WHEN COALESCE($3, '') = '' THEN resolution_notes
                WHEN resolution_notes IS NULL OR resolution_notes = '' THEN $3
                ELSE resolution_notes || E'
' || $3
            END
        WHERE code = $1
        RETURNING *
        """,
        market_code,
        STATUS_CLOSED,
        close_reason,
    )
    updated_market = _normalize_market_row(dict(row))
    snapshot = await insert_snapshot(conn, updated_market)
    return {
        "market": updated_market,
        "snapshot": snapshot,
        "closed_by": closed_by,
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
        SELECT id, discord_uuid, amount, membership_tier_at_wager, fee_rate_bps_at_wager
        FROM prediction_wagers
        WHERE market_code = $1
          AND settled_at IS NULL
        ORDER BY id ASC
        """,
        market_code,
    )

    settlement_rows: List[Dict[str, Any]] = []

    for wager in wagers:
        amount = _quantize(_to_decimal(wager["amount"]))
        await balance_repo.get_balance_for_update(conn, wager["discord_uuid"])
        await balance_repo.add_balance(conn, wager["discord_uuid"], amount)
        await balance_repo.insert_balance_transaction(
            conn,
            discord_uuid=wager["discord_uuid"],
            kind="prediction_refund",
            amount=amount,
            applied_rates={
                "prediction_fee_rate_bps": 0,
                "prediction_fee_percent": 0,
            },
            metadata={
                "market_code": market_code,
                "wager_id": int(wager["id"]),
                "result": "CANCELLED",
                "membership_tier_at_wager": str(wager.get("membership_tier_at_wager") or "free").lower(),
                "gross_payout_amount": str(amount),
                "profit_amount": "0",
                "fee_amount": "0",
                "net_payout_amount": str(amount),
            },
        )
        await conn.execute(
            """
            UPDATE prediction_wagers
            SET payout_amount = $2,
                gross_payout_amount = $3,
                profit_amount = 0,
                fee_amount = 0,
                outcome = 'CANCELLED',
                settled_at = NOW(),
                notification_attempts = 0,
                notified_at = NULL,
                notification_last_error = NULL
            WHERE id = $1
            """,
            wager["id"],
            amount,
            amount,
        )
        settlement_rows.append(
            {
                "wager_id": int(wager["id"]),
                "discord_uuid": str(wager["discord_uuid"]),
                "amount": amount,
                "outcome": "CANCELLED",
                "gross_payout_amount": amount,
                "profit_amount": ZERO,
                "fee_amount": ZERO,
                "net_payout_amount": amount,
            }
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
        "settlements": settlement_rows,
    }


async def list_pending_settlement_notifications(conn, *, limit: int = 50) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            w.id AS wager_id,
            w.market_code,
            w.discord_uuid,
            w.side,
            w.amount,
            w.payout_amount,
            w.gross_payout_amount,
            w.profit_amount,
            w.fee_amount,
            w.outcome,
            w.membership_tier_at_wager,
            w.fee_rate_bps_at_wager,
            w.created_at,
            w.settled_at,
            m.title AS market_title,
            m.outcome AS market_outcome,
            m.status AS market_status
        FROM prediction_wagers w
        JOIN prediction_markets m
          ON m.code = w.market_code
        WHERE w.settled_at IS NOT NULL
          AND w.notified_at IS NULL
          AND (
                (w.outcome = 'WIN' AND w.payout_amount > 0)
                OR w.outcome = 'CANCELLED'
              )
        ORDER BY w.settled_at ASC, w.id ASC
        LIMIT $1
        """,
        limit,
    )
    return [_normalize_wager_row(dict(row)) for row in rows]


async def mark_wager_notification_delivered(conn, wager_id: int) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE prediction_wagers
        SET notified_at = NOW(),
            notification_last_error = NULL
        WHERE id = $1
        RETURNING *
        """,
        wager_id,
    )
    return _normalize_wager_row(dict(row)) if row else None


async def mark_wager_notification_failed(conn, wager_id: int, error: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        """
        UPDATE prediction_wagers
        SET notification_attempts = notification_attempts + 1,
            notification_last_error = $2
        WHERE id = $1
        RETURNING *
        """,
        wager_id,
        error,
    )
    return _normalize_wager_row(dict(row)) if row else None

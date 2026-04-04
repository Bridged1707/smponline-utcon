from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field


DepositChallengeStatus = Literal["pending", "matched", "expired", "failed", "cancelled"]


class DepositChallengeCreateRequest(BaseModel):
    discord_uuid: str = Field(min_length=1, max_length=64)


class DepositChallengeResolveRequest(BaseModel):
    queue_id: int
    matched_transaction_id: int
    processed_by: Optional[str] = None


class DepositChallengeFailRequest(BaseModel):
    queue_id: int
    status: Literal["failed", "expired", "cancelled"]
    failure_reason: Optional[str] = None
    processed_by: Optional[str] = None


class DepositChallengeResponse(BaseModel):
    queue_id: int
    discord_uuid: str
    challenge_shop_id: int
    challenge_owner_uuid: str
    challenge_owner_name: str
    challenge_item_type: str
    challenge_item_name: Optional[str] = None
    challenge_item_quantity: int
    challenge_price: Decimal
    expected_total: Decimal
    challenge_world: str
    challenge_x: int
    challenge_y: int
    challenge_z: int
    status: DepositChallengeStatus
    requested_at: datetime
    expires_at: datetime
    resolved_at: Optional[datetime] = None
    matched_transaction_id: Optional[int] = None
    failure_reason: Optional[str] = None
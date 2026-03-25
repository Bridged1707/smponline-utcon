from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


MembershipTier = Literal["free", "pro", "garry"]


class MembershipLookupResponse(BaseModel):
    discord_uuid: str
    membership_type: Literal["unregistered", "free", "pro", "garry"]
    tier: Optional[MembershipTier] = None
    starts_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    is_active: bool
    is_registered: bool
    source: str


class MembershipUpsertRequest(BaseModel):
    discord_uuid: str
    tier: MembershipTier
    duration_days: int = Field(default=7, ge=1, le=3650)
    reason: Optional[str] = None
    replace_active: bool = True


class MembershipPurchaseRequest(BaseModel):
    discord_uuid: str
    tier: Literal["pro", "garry"]
    weeks: Optional[int] = Field(default=None, ge=1, le=520)
    amount: Optional[int] = Field(default=None, ge=1, le=1000000000)

    @model_validator(mode="after")
    def validate_amount_or_weeks(self):
        if self.weeks is None and self.amount is None:
            raise ValueError("either weeks or amount must be provided")
        if self.weeks is not None and self.amount is not None:
            raise ValueError("provide either weeks or amount, not both")
        return self
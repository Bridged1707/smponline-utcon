from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


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
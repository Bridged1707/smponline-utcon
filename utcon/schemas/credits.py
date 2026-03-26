from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CommandCreditConsumeRequest(BaseModel):
    discord_uuid: str = Field(min_length=1)
    command: str = Field(min_length=1)
    dry_run: bool = False
    metadata: dict[str, Any] | None = None


class CommandCreditConsumeResponse(BaseModel):
    allowed: bool
    reason: str | None = None
    discord_uuid: str
    command: str
    tier: str
    charged_credits: int
    dry_run: bool
    weekly_credits: int
    used_credits: int
    remaining_credits: int
    week_start_at: datetime
    next_reset_at: datetime
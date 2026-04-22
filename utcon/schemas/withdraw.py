from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class WithdrawResolveRequest(BaseModel):
    discord_uuid: str
    processed_by: Optional[str] = None
    notes: Optional[str] = None

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class BalanceRequest(BaseModel):
    discord_uuid: str
    amount: float


class AdminBalanceAdjustRequest(BaseModel):
    discord_uuid: str
    amount: float
    reference: Optional[str] = None
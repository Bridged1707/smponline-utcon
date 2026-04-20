from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class BalanceNotificationCreateRequest(BaseModel):
    discord_uuid: str
    amount: float
    reason: Optional[str] = None
    source: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BalanceNotificationDeliveryResultRequest(BaseModel):
    error: Optional[str] = None
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field


PredictionStatus = Literal["open", "closed", "resolved", "cancelled"]
PredictionSide = Literal["YES", "NO"]
PredictionOutcome = Literal["YES", "NO", "CANCELLED"]


class PredictionMarketCreateRequest(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    title: str = Field(min_length=1, max_length=300)
    description: Optional[str] = None
    closes_at: datetime
    resolves_at: Optional[datetime] = None
    created_by: Optional[str] = None


class PredictionWagerRequest(BaseModel):
    discord_uuid: str
    market_code: str = Field(min_length=1, max_length=64)
    side: PredictionSide
    amount: Decimal = Field(gt=0)


class PredictionResolveRequest(BaseModel):
    outcome: PredictionOutcome
    resolved_by: Optional[str] = None
    resolution_notes: Optional[str] = None


class PredictionCancelRequest(BaseModel):
    cancelled_by: Optional[str] = None
    reason: Optional[str] = None


class PredictionNotificationFailureRequest(BaseModel):
    error: Optional[str] = None

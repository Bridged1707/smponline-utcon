from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field


class PredictionMarketType:
    BINARY = "binary"
    CATEGORICAL = "categorical"
    NUMERIC_RANGE = "numeric_range"
    NUMERIC_EXACT = "numeric_exact"


class PredictionResolutionMode:
    MANUAL = "manual"
    ADMIN_SET_OPTION = "admin_set_option"
    ADMIN_SET_NUMERIC = "admin_set_numeric"


class PredictionMarketOption(BaseModel):
    id: int
    option_code: str
    label: str
    sort_order: int
    is_active: bool
    is_resolved_winner: bool
    implied_price: Decimal = Decimal("0")
    pool_amount: Decimal = Decimal("0")


class PredictionMarketCreateOption(BaseModel):
    option_code: str
    label: str
    sort_order: int = 0


class PredictionMarketCreateRequest(BaseModel):
    code: str
    title: str
    description: Optional[str] = None
    market_type: str = PredictionMarketType.BINARY
    resolution_mode: str = PredictionResolutionMode.MANUAL
    closes_at: Optional[str] = None
    resolves_at: Optional[str] = None
    created_by: Optional[str] = None
    options: Optional[List[PredictionMarketCreateOption]] = None


class PredictionMarketResponse(BaseModel):
    code: str
    title: str
    description: Optional[str] = None
    market_type: str
    status: str
    options: List[PredictionMarketOption] = Field(default_factory=list)
    total_volume: Decimal = Decimal("0")


class PredictionWagerRequest(BaseModel):
    discord_uuid: str
    market_code: str
    option_code: str
    amount: Decimal


class PredictionResolveRequest(BaseModel):
    market_code: str
    option_code: Optional[str] = None
    resolution_notes: Optional[str] = None
    resolved_by: Optional[str] = None


class PredictionCancelRequest(BaseModel):
    market_code: str
    cancelled_by: Optional[str] = None
    reason: Optional[str] = None


class PredictionCloseRequest(BaseModel):
    market_code: str
    closed_by: Optional[str] = None


class PredictionLookupRequest(BaseModel):
    market_code: str


class PredictionListRequest(BaseModel):
    status: Optional[str] = None
    include_closed: bool = False
    limit: int = 25


class PredictionUserWagersRequest(BaseModel):
    discord_uuid: str
    unsettled_only: bool = True
    limit: int = 25
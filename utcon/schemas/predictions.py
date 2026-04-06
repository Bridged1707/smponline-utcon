from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, validator


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
    trade_volume: Decimal = Decimal("0")
    wager_count: int = 0
    numeric_value: Optional[Decimal] = None
    range_min: Optional[Decimal] = None
    range_max: Optional[Decimal] = None
    range_min_inclusive: bool = True
    range_max_inclusive: bool = False


class PredictionMarketCreateOption(BaseModel):
    option_code: str
    label: str
    sort_order: int = 0
    description: Optional[str] = None
    numeric_value: Optional[Decimal] = None
    range_min: Optional[Decimal] = None
    range_max: Optional[Decimal] = None
    range_min_inclusive: bool = True
    range_max_inclusive: bool = False


class PredictionMarketCreateRequest(BaseModel):
    code: str
    title: str
    description: Optional[str] = None
    market_type: str = PredictionMarketType.BINARY
    resolution_mode: str = PredictionResolutionMode.MANUAL
    closes_at: Optional[datetime] = None
    resolves_at: Optional[datetime] = None
    created_by: Optional[str] = None
    options: Optional[List[PredictionMarketCreateOption]] = None

    @validator("closes_at", "resolves_at", pre=True)
    def _parse_optional_datetime(cls, value):
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        text = str(value).strip()
        if not text:
            return None

        candidates = (
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%dT%H:%M:%S",
        )
        for fmt in candidates:
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                pass

        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("invalid datetime format; use YYYY-MM-DD HH:MM")


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
    membership_tier: Optional[str] = None


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
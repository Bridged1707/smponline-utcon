from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field


# =========================
# ENUMS
# =========================

class PredictionMarketType(str):
    BINARY = "binary"
    CATEGORICAL = "categorical"
    NUMERIC_RANGE = "numeric_range"
    NUMERIC_EXACT = "numeric_exact"


class PredictionResolutionMode(str):
    MANUAL = "manual"
    ADMIN_SET_OPTION = "admin_set_option"
    ADMIN_SET_NUMERIC = "admin_set_numeric"


# =========================
# OPTION MODELS
# =========================

class PredictionMarketOption(BaseModel):
    id: int
    option_code: str
    label: str
    sort_order: int
    is_active: bool
    is_resolved_winner: bool
    implied_price: Decimal = 0
    pool_amount: Decimal = 0


class PredictionMarketCreateOption(BaseModel):
    option_code: str
    label: str
    sort_order: int = 0


# =========================
# MARKET MODELS
# =========================

class PredictionMarketCreateRequest(BaseModel):
    code: str
    title: str
    description: Optional[str] = None
    market_type: str = PredictionMarketType.BINARY
    resolution_mode: str = PredictionResolutionMode.MANUAL
    closes_at: Optional[str] = None

    options: Optional[List[PredictionMarketCreateOption]] = None


class PredictionMarketResponse(BaseModel):
    code: str
    title: str
    description: Optional[str]
    market_type: str
    status: str

    options: List[PredictionMarketOption]

    total_volume: Decimal


# =========================
# WAGER
# =========================

class PredictionWagerRequest(BaseModel):
    discord_uuid: str
    market_code: str
    option_code: str
    amount: Decimal


class PredictionResolveRequest(BaseModel):
    market_code: str
    option_code: Optional[str] = None
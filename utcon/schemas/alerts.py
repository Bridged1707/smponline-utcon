from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


AlertType = Literal[
    "NEW_SHOP",
    "SHOP_SALE",
    "SHOP_INVENTORY_UPDATE",
    "SHOP_PRICE_UPDATE",
    "AUCTION_SALE",
    "SYMBOL_PRICE",
]

TargetType = Literal["ITEM", "SYMBOL"]


class AlertCreateRequest(BaseModel):
    discord_uuid: str = Field(min_length=1, max_length=64)
    alert_type: AlertType
    target_type: TargetType
    target_key: str = Field(min_length=1, max_length=128)
    target_name: Optional[str] = None
    snbt: Optional[str] = None
    min_threshold: Optional[float] = None
    max_threshold: Optional[float] = None
    stock_minimum: Optional[int] = None
    stock_maximum: Optional[int] = None
    world: Optional[str] = None
    x: Optional[int] = None
    y: Optional[int] = None
    z: Optional[int] = None
    cooldown_seconds: int = Field(default=300, ge=0)
    notes: Optional[str] = None


class AlertToggleRequest(BaseModel):
    discord_uuid: str = Field(min_length=1, max_length=64)
    is_active: bool


class AlertStateUpsertRequest(BaseModel):
    alert_id: int
    state_key: str = Field(min_length=1, max_length=255)
    last_seen_ts: Optional[int] = None
    last_event_ts: Optional[int] = None
    last_seen_price: Optional[float] = None
    last_seen_remaining: Optional[int] = None
    last_in_band: Optional[bool] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AlertEventCreateRequest(BaseModel):
    alert_id: int
    discord_uuid: str = Field(min_length=1, max_length=64)
    event_type: str = Field(min_length=1, max_length=64)
    title: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=4000)
    source_key: Optional[str] = None
    dedupe_key: str = Field(min_length=1, max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict)


class AlertEventDeliveryResultRequest(BaseModel):
    error: Optional[str] = None
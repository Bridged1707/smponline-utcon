from __future__ import annotations

from pydantic import BaseModel, Field


class CasinoUserRegisterRequest(BaseModel):
    sender_external_id: str = Field(min_length=1, max_length=255)


class CasinoBalanceUpdateRequest(BaseModel):
    amount_delta: int


class CasinoPfSaveRequest(BaseModel):
    client_seed: str = Field(min_length=1, max_length=255)
    server_seed: str = Field(min_length=1, max_length=255)
    nonce: int = Field(ge=0)


class CasinoFinancialTransactionAppendRequest(BaseModel):
    type: str = Field(min_length=1, max_length=64)
    amount: int
    net_amount: int


class CasinoAccountPanelStateRequest(BaseModel):
    message_id: int


class CasinoTableCreateRequest(BaseModel):
    channel_id: int
    category_id: int
    table_number: int = Field(ge=1)
    channel_name: str = Field(min_length=1, max_length=255)
    category_name: str = Field(min_length=1, max_length=255)


from typing import Any


class CasinoGameSessionStartRequest(BaseModel):
    discord_uuid: str = Field(min_length=1, max_length=255)
    game_type: str = Field(min_length=1, max_length=64)
    wager_amount: float = Field(gt=0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CasinoGameSessionSettleRequest(BaseModel):
    gross_payout_amount: float = Field(ge=0)
    outcome: str = Field(min_length=1, max_length=32)
    membership_tier: str | None = Field(default=None, max_length=32)
    metadata: dict[str, Any] = Field(default_factory=dict)

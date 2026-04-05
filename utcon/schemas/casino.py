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

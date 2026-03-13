from pydantic import BaseModel
from typing import Optional


class BalanceLookupResponse(BaseModel):
    discord_uuid: str
    balance: float


class DepositRequest(BaseModel):
    discord_uuid: str
    amount: float


class WithdrawRequest(BaseModel):
    discord_uuid: str
    amount: float


class TransferRequest(BaseModel):
    from_discord_uuid: str
    to_discord_uuid: str
    amount: float


class TopupRequest(BaseModel):
    discord_uuid: str
    amount: float
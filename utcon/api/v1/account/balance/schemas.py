# schemas.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class LookupResponse(BaseModel):
    discord_uuid: str
    balance: float
    roles: list[str]
    rates: Dict[str, Any]

class TransferRequest(BaseModel):
    discord_uuid: str
    amount: float = Field(..., gt=0)
    operator: Optional[str] = None
    reason: Optional[str] = None

class TransferBetweenRequest(BaseModel):
    from_discord_uuid: str
    to_discord_uuid: str
    amount: float = Field(..., gt=0)
    operator: Optional[str] = None
    reason: Optional[str] = None

class TopupRequest(BaseModel):
    discord_uuid: str
    amount: float # can be positive (credit) or negative (debit)
    operator: Optional[str] = None
    reason: Optional[str] = None
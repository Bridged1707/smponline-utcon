from pydantic import BaseModel


class BalanceRequest(BaseModel):
    discord_uuid: str
    amount: float
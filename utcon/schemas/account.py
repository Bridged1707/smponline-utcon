from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class AccountAddRequest(BaseModel):
    discord_uuid: str
    mc_uuid: str
    mc_name: Optional[str] = None


class AccountRemoveRequest(BaseModel):
    discord_uuid: str


class DiscordSRVRegisterRequest(BaseModel):
    discord_uuid: str
    mc_uuid: str
    mc_name: str
    source: str = "discordsrv-command"
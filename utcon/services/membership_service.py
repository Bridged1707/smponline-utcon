from __future__ import annotations

from typing import Any

from smpbot.clients.utcon import UTCONClient


class MembershipService:
    def __init__(self, utcon: UTCONClient):
        self.utcon = utcon

    async def get_membership(self, discord_uuid: str) -> dict[str, Any]:
        return await self.utcon.get_membership(discord_uuid)
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class WithdrawResolveRequest(BaseModel):
    discord_uuid: str
    processed_by: Optional[str] = None
    notes: Optional[str] = None
    withdrawal_id: Optional[int] = None
    id: Optional[int] = None

    @property
    def effective_withdrawal_id(self) -> Optional[int]:
        return self.withdrawal_id if self.withdrawal_id is not None else self.id

from pydantic import BaseModel


class RegisterAccount(BaseModel):
    discord_uuid: str
    mc_uuid: str
    mc_name: str

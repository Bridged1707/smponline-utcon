from fastapi import APIRouter
from pydantic import BaseModel

from utcon import db
from utcon.repositories import account as account_repo

router = APIRouter(prefix="/v1/account", tags=["account"])


class RegisterRequest(BaseModel):
    discord_uuid: str
    mc_uuid: str
    mc_name: str


@router.post("/register")
async def register(req: RegisterRequest):

    async with db.connection() as conn:

        await account_repo.create_account(
            conn,
            req.discord_uuid,
            req.mc_uuid,
            req.mc_name,
        )

    return {"status": "account_created"}
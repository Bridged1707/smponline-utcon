import requests
from fastapi import APIRouter, HTTPException
from utcon.schemas.account import RegisterAccount

router = APIRouter()

UTDB_URL = "http://10.1.0.91:9000"


@router.post("/register")
def register_account(account: RegisterAccount):

    try:
        r = requests.post(
            f"{UTDB_URL}/accounts/register",
            json=account.dict(),
            timeout=5
        )

        r.raise_for_status()
        return r.json()

    except requests.exceptions.HTTPError as http_err:
        raise HTTPException(status_code=r.status_code, detail=f"Registration failed: {r.text}")
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=503, detail="Unable to reach registration service")
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Registration service timeout")
    except requests.exceptions.JSONDecodeError:
        raise HTTPException(status_code=502, detail="Invalid response from registration service")
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(err)}")
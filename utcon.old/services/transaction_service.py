import requests
from utcon.core.config import UTDB_API_URL


def lookup_auctions(filters):

    response = requests.get(
        f"{UTDB_API_URL}/transactions/lookup/auction",
        params=filters,
        timeout=10
    )
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        return {"error": f"HTTP error: {http_err}", "status_code": response.status_code, "content": response.text}
    except requests.exceptions.JSONDecodeError as json_err:
        return {"error": f"Invalid JSON response: {json_err}", "status_code": response.status_code, "content": response.text}
    except Exception as err:
        return {"error": f"Unexpected error: {err}"}


def lookup_shop_transactions(filters):

    response = requests.get(
        f"{UTDB_API_URL}/transactions/lookup/shop",
        params=filters,
        timeout=10
    )
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        return {"error": f"HTTP error: {http_err}", "status_code": response.status_code, "content": response.text}
    except requests.exceptions.JSONDecodeError as json_err:
        return {"error": f"Invalid JSON response: {json_err}", "status_code": response.status_code, "content": response.text}
    except Exception as err:
        return {"error": f"Unexpected error: {err}"}
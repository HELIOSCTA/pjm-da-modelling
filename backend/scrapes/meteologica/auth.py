"""
Meteologica xTraders API authentication module.

Manages JWT token lifecycle (obtain, refresh, auto-renew) for the Meteologica
Markets API. Supports two accounts:
    - "l48"  : Lower 48 (US48 aggregate) -- XTRADERS_API_USERNAME_L48
    - "iso"  : ISO-level (PJM, ERCOT, etc.) -- XTRADERS_API_USERNAME_ISO

Token is held in memory only -- no disk writes.

Usage:
    from backend.scrapes.meteologica.auth import get_token, make_get_request

    # L48 (default -- backward-compatible)
    token = get_token()
    resp  = make_get_request("contents/4226/data")

    # ISO
    token = get_token(account="iso")
    resp  = make_get_request("contents/1234/data", account="iso")
"""

import time
import logging
from typing import Literal

import jwt
import requests

from backend import secrets

logger = logging.getLogger(__name__)

BASE_URL = "https://api-markets.meteologica.com/api/v1/"

Account = Literal["l48", "iso"]

# Credentials lookup by account
_CREDENTIALS: dict[Account, tuple[str, str]] = {
    "l48": (
        secrets.XTRADERS_API_USERNAME_L48,
        secrets.XTRADERS_API_PASSWORD_L48,
    ),
    "iso": (
        secrets.XTRADERS_API_USERNAME_ISO,
        secrets.XTRADERS_API_PASSWORD_ISO,
    ),
}

# In-memory token store (one per account)
_cached_tokens: dict[Account, str | None] = {"l48": None, "iso": None}


def _get_new_token(account: Account = "l48") -> str:
    """Authenticate with username/password and return a fresh JWT token."""
    username, password = _CREDENTIALS[account]
    response = requests.post(
        f"{BASE_URL}login",
        json={"user": username, "password": password},
        timeout=30,
    )
    response.raise_for_status()

    try:
        return response.json()["token"]
    except (KeyError, TypeError) as e:
        raise RuntimeError(
            f"Could not extract token from login response: "
            f"{response.text} ({response.status_code})"
        ) from e


def _refresh_token(token: str) -> str:
    """Refresh an existing token via the keepalive endpoint."""
    response = requests.get(
        f"{BASE_URL}keepalive",
        params={"token": token},
        timeout=30,
    )
    response.raise_for_status()

    try:
        return response.json()["token"]
    except (KeyError, TypeError) as e:
        raise RuntimeError(
            f"Could not extract token from keepalive response: "
            f"{response.text} ({response.status_code})"
        ) from e


def _is_expired(token: str) -> bool:
    """Check if the JWT token has already expired."""
    payload = jwt.decode(token, options={"verify_signature": False})
    return time.time() > payload["exp"]


def _is_expiring_soon(token: str, threshold_seconds: int = 300) -> bool:
    """Check if the JWT token will expire within `threshold_seconds`."""
    payload = jwt.decode(token, options={"verify_signature": False})
    remaining = payload["exp"] - time.time()
    return 0 < remaining < threshold_seconds


def get_token(account: Account = "l48") -> str:
    """
    Return a valid JWT token, obtaining or refreshing as needed.

    Args:
        account: "l48" for Lower-48 data, "iso" for ISO-level data.
    """
    global _cached_tokens

    cached = _cached_tokens[account]

    # Case 1: No token or expired token -> login fresh
    if cached is None or _is_expired(cached):
        logger.info(f"Obtaining new Meteologica API token (account={account})")
        _cached_tokens[account] = _get_new_token(account)
        return _cached_tokens[account]

    # Case 2: Token expiring soon -> refresh
    if _is_expiring_soon(cached):
        logger.info(f"Refreshing Meteologica API token (account={account})")
        _cached_tokens[account] = _refresh_token(cached)
        return _cached_tokens[account]

    # Case 3: Token is still valid
    return cached


def make_get_request(
    endpoint: str,
    params: dict | None = None,
    account: Account = "l48",
) -> requests.Response:
    """
    Make an authenticated GET request to the Meteologica API.

    Automatically injects the current token into query params.

    Args:
        endpoint: API path (e.g. "contents/4226/data").
        params:   Extra query parameters.
        account:  "l48" or "iso".
    """
    token = get_token(account)
    query_params = {"token": token}
    if params:
        query_params.update(params)

    response = requests.get(f"{BASE_URL}{endpoint}", params=query_params, timeout=60)
    response.raise_for_status()
    return response

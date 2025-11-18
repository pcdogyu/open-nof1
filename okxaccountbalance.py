"""
Fetch OKX demo account balance using credentials stored in config.py.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Dict

import requests

from config import OKX_ACCOUNTS

OKX_API_BASE = "https://www.okx.com"
BALANCE_ENDPOINT = "/api/v5/account/balance"


def _generate_signature(secret_key: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """Create OKX HMAC-SHA256 signature."""
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret_key.encode(), payload.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def _build_headers(credentials: Dict[str, str], timestamp: str, signature: str) -> Dict[str, str]:
    """Compose HTTP headers required by OKX private endpoints."""
    return {
        "OK-ACCESS-KEY": credentials["api_key"],
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": credentials["passphrase"],
        "x-simulated-trading": "1",
        "Content-Type": "application/json",
    }


def fetch_balance(account_key: str = "moni") -> Dict:
    """Fetch balance for a configured OKX account."""
    credentials = OKX_ACCOUNTS[account_key]
    timestamp = f"{time.time():.3f}"
    signature = _generate_signature(
        secret_key=credentials["api_secret"],
        timestamp=timestamp,
        method="GET",
        request_path=BALANCE_ENDPOINT,
    )
    headers = _build_headers(credentials, timestamp, signature)
    response = requests.get(f"{OKX_API_BASE}{BALANCE_ENDPOINT}", headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    try:
        result = fetch_balance()
        print("OKX balance response:")
        print(result)
    except Exception as exc:  # pylint: disable=broad-except
        print("Failed to fetch balance:", exc)

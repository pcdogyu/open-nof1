"""
OKX paper-trading client with REST order and cancel support.

This adapter targets the official OKX Demo/Testnet environment. When
``simulate=True`` a ``x-api-simulate: 1`` header is included so real funds are
never touched.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Literal, Optional

import httpx

from exchanges.base_client import ExchangeClient, ExchangeCredentials

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class OkxClientError(RuntimeError):
    """Raised when OKX returns a non-success response."""


@dataclass(slots=True)
class OrderPayload:
    """Normalized order payload accepted by the client."""

    instrument_id: str
    side: Literal["buy", "sell"]
    order_type: Literal["limit", "market"]
    size: str
    margin_mode: Literal["cash", "cross", "isolated"] = "cash"
    price: Optional[str] = None
    client_order_id: Optional[str] = None


class OkxPaperClient(ExchangeClient):
    """Paper trading client for OKX REST API."""

    name = "okx-paper"

    def __init__(
        self,
        base_url: str | None = None,
        simulate: bool = True,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = base_url or "https://www.okx.com"
        self._simulate = simulate
        self._client = httpx.Client(base_url=self._base_url, timeout=timeout)
        self._credentials: ExchangeCredentials | None = None

    # ---------------------------------------------------------------------
    # ExchangeClient API
    # ---------------------------------------------------------------------
    def authenticate(self, credentials: ExchangeCredentials) -> None:
        self._credentials = credentials
        # Probe private endpoint to ensure credentials are valid.
        self.fetch_balances()

    def fetch_balances(self) -> dict:
        response = self._request("GET", "/api/v5/account/balance")
        return response.get("data", [])

    def fetch_positions(self, symbols: Iterable[str] | None = None) -> list[dict]:
        params = {}
        if symbols:
            # OKX expects comma-separated instrument IDs under instId.
            params["instId"] = ",".join(symbols)
        response = self._request("GET", "/api/v5/account/positions", params=params)
        return response.get("data", [])

    def place_order(self, payload: dict) -> dict:
        order = self._normalize_order_payload(payload)
        body = {
            "instId": order.instrument_id,
            "tdMode": order.margin_mode,
            "side": order.side,
            "ordType": order.order_type,
            "sz": order.size,
        }
        if order.price and order.order_type == "limit":
            body["px"] = order.price
        if order.client_order_id:
            body["clOrdId"] = order.client_order_id
        response = self._request("POST", "/api/v5/trade/order", json_body=body)
        data = _single_item(response)
        return {
            "status": "submitted",
            "order_id": data.get("ordId"),
            "client_order_id": data.get("clOrdId"),
            "instrument_id": order.instrument_id,
            "raw": response,
        }

    def cancel_order(self, order_id: str, instrument_id: Optional[str] = None) -> dict:
        if not instrument_id:
            raise ValueError("instrument_id is required to cancel an OKX order")
        body = {
            "instId": instrument_id,
            "ordId": order_id,
        }
        response = self._request("POST", "/api/v5/trade/cancel-order", json_body=body)
        data = _single_item(response)
        return {
            "status": "cancelled",
            "order_id": data.get("ordId"),
            "instrument_id": instrument_id,
            "raw": response,
        }

    def close(self) -> None:
        self._client.close()
        self._credentials = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _request(
        self,
        method: Literal["GET", "POST"],
        path: str,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
    ) -> dict:
        if self._credentials is None:
            raise RuntimeError("Client has not been authenticated")

        timestamp = datetime.now(tz=timezone.utc).strftime(ISO_FORMAT)
        body_text = json.dumps(json_body, separators=(",", ":")) if json_body else ""
        query = ""
        if params:
            query = httpx.QueryParams(params).render()
            path_with_params = f"{path}?{query}"
        else:
            path_with_params = path

        message = f"{timestamp}{method}{path_with_params}{body_text}"
        signature = self._sign(message, self._credentials.api_secret)

        headers = {
            "OK-ACCESS-KEY": self._credentials.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._credentials.passphrase or "",
            "Content-Type": "application/json",
        }
        if self._simulate:
            headers["x-api-simulate"] = "1"

        response = self._client.request(
            method,
            path,
            params=params,
            content=body_text if body_text else None,
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "0":
            raise OkxClientError(
                f"OKX error {payload.get('code')}: {payload.get('msg')}"
            )
        return payload

    @staticmethod
    def _sign(message: str, secret_key: str) -> str:
        mac = hmac.new(
            secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    @staticmethod
    def _normalize_order_payload(raw: dict) -> OrderPayload:
        try:
            order = OrderPayload(
                instrument_id=raw["instrument_id"],
                side=raw["side"],
                order_type=raw["order_type"],
                size=str(raw["size"]),
                margin_mode=raw.get("margin_mode", "cash"),
                price=str(raw["price"]) if raw.get("price") else None,
                client_order_id=raw.get("client_order_id"),
            )
        except KeyError as exc:
            raise ValueError(f"Missing required order field: {exc}") from exc
        if order.order_type == "limit" and not order.price:
            raise ValueError("Limit orders require a price")
        return order


def _single_item(response: dict) -> dict:
    data = response.get("data") or []
    if not data:
        raise OkxClientError("OKX returned empty data payload")
    return data[0]

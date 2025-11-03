"""
Utility helpers for managing the OKX instrument catalog.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable, List

import httpx

from data_pipeline.influx import InfluxConfig, InfluxWriter

CATALOG_CACHE_PATH = Path("data/state/okx_instruments.json")
OKX_INSTRUMENT_ENDPOINT = "/api/v5/public/instruments"
OKX_DEFAULT_PARAMS = {"instType": "SWAP"}


async def fetch_okx_swap_instruments() -> list[dict]:
    """Fetch the list of swap instruments supported by OKX."""
    async with httpx.AsyncClient(base_url="https://www.okx.com", timeout=10.0) as client:
        response = await client.get(OKX_INSTRUMENT_ENDPOINT, params=OKX_DEFAULT_PARAMS)
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX instruments API error {payload.get('code')}: {payload.get('msg')}")
        return payload.get("data", [])


def persist_catalog_cache(instruments: Iterable[dict]) -> None:
    """Persist the fetched instruments to a local cache file for quick lookup."""
    CATALOG_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": time.time(),
        "instruments": list(instruments),
    }
    CATALOG_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_catalog_cache() -> list[dict]:
    """Load cached instruments from disk (if present)."""
    if not CATALOG_CACHE_PATH.exists():
        return []
    try:
        payload = json.loads(CATALOG_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return payload.get("instruments", [])


def write_catalog_to_influx(instruments: Iterable[dict]) -> bool:
    """Write the instrument catalog into InfluxDB for auditing/search purposes."""
    try:
        config = InfluxConfig.from_env()
        writer = InfluxWriter(config)
    except ValueError:
        return False

    timestamp = int(time.time() * 1e9)
    try:
        for entry in instruments:
            inst_id = entry.get("instId")
            if not inst_id:
                continue
            tags = {
                "instrument_id": inst_id,
                "inst_type": entry.get("instType", ""),
                "category": entry.get("category", ""),
                "settle_currency": entry.get("settleCcy", ""),
                "state": entry.get("state", ""),
            }
            fields: dict[str, Any] = {
                "base_currency": entry.get("baseCcy", ""),
                "quote_currency": entry.get("quoteCcy", ""),
                "alias": entry.get("alias", ""),
                "lot_size": entry.get("lotSz", ""),
                "ct_val": entry.get("ctVal", ""),
                "exp_time": entry.get("expTime", ""),
            }
            writer.write_indicator_set(
                measurement="instrument_catalog",
                tags=tags,
                fields=fields,
                timestamp_ns=timestamp,
            )
    finally:
        writer.close()
    return True


async def refresh_okx_instrument_catalog() -> dict:
    """Fetch instruments from OKX, persist to cache and Influx, and return the list."""
    instruments = await fetch_okx_swap_instruments()
    persist_catalog_cache(instruments)
    wrote_influx = write_catalog_to_influx(instruments)
    return {
        "count": len(instruments),
        "wrote_influx": wrote_influx,
        "instruments": instruments,
    }


def search_instrument_catalog(
    query: str | None = None,
    *,
    limit: int = 50,
    quote: str | None = None,
) -> list[dict]:
    """Perform a case-insensitive fuzzy search over the cached catalog.

    - Filters by `quote` currency when provided (e.g., "USDT", "USD").
    - Matches against instrument id, alias, base currency, and quote currency.
    - Lightly ranks results to prefer base-currency prefix matches.
    """
    catalog = load_catalog_cache()
    if not query:
        # If only quote is provided, filter by it; else return a small slice.
        if quote:
            qsym = quote.strip().lower()
            out: list[dict] = []
            for e in catalog:
                if str(e.get("quoteCcy", "")).lower() == qsym:
                    out.append(e)
                    if len(out) >= limit:
                        break
            return out
        return catalog[:limit]

    q = (query or "").strip().lower()
    quote_sym = (quote or "").strip().lower() if quote else None
    ranked: list[tuple[int, dict]] = []
    for entry in catalog:
        inst_id = str(entry.get("instId", "")).lower()
        alias = str(entry.get("alias", "")).lower()
        base = str(entry.get("baseCcy", "")).lower()
        quote = str(entry.get("quoteCcy", "")).lower()

        if quote_sym and quote != quote_sym:
            continue

        score: int | None = None
        if base.startswith(q):
            score = 0
        elif alias.startswith(q) or inst_id.startswith(q):
            score = 1
        elif q in base or q in quote:
            score = 2
        elif q in inst_id or (alias and q in alias):
            score = 3

        if score is not None:
            ranked.append((score, entry))
            if len(ranked) >= limit * 4:  # gather a bit extra before trimming
                break

    ranked.sort(key=lambda item: (item[0], str(item[1].get("instId", ""))))
    return [entry for _, entry in ranked[:limit]]

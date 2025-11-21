"""
InfluxDB writer utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List
import json
import os
import random

from influxdb_client import InfluxDBClient, Point, WritePrecision


@dataclass(slots=True)
class InfluxConfig:
    """Configuration required to connect to InfluxDB."""

    url: str = "http://localhost:8086"
    token: str | None = None
    org: str = "nof"
    bucket: str = "nof"

    @staticmethod
    def from_env() -> "InfluxConfig":
        token = os.getenv("INFLUX_TOKEN")
        url = os.getenv("INFLUX_URL")
        org = os.getenv("INFLUX_ORG")
        bucket = os.getenv("INFLUX_BUCKET")
        tokens_env = os.getenv("INFLUX_TOKENS")
        if tokens_env:
            candidates = [
                t.strip()
                for t in tokens_env.split(",")
                if t.strip() and not t.upper().startswith("REPLACE")
            ]
            if candidates:
                token = random.choice(candidates)

        chosen_profile: dict[str, str] | None = None

        if not token:
            try:  # fallback to config.py
                import config as config_module  # type: ignore

                profile_pool: list[dict[str, str]] = []
                if hasattr(config_module, "INFLUX_PROFILES"):
                    raw_profiles = getattr(config_module, "INFLUX_PROFILES")
                    if isinstance(raw_profiles, (list, tuple)):
                        for profile in raw_profiles:
                            if not isinstance(profile, dict):
                                continue
                            if not profile.get("enabled", True):
                                continue
                            token_candidate = profile.get("token")
                            if token_candidate and not str(token_candidate).upper().startswith("REPLACE"):
                                profile_pool.append(profile)
                if profile_pool:
                    chosen_profile = random.choice(profile_pool)
                    token = chosen_profile.get("token")
                else:
                    token_pool = []
                    if hasattr(config_module, "INFLUX_TOKENS"):
                        raw_tokens = getattr(config_module, "INFLUX_TOKENS")
                        if isinstance(raw_tokens, (list, tuple)):
                            token_pool.extend(
                                [t for t in raw_tokens if t and not str(t).upper().startswith("REPLACE")]
                            )
                    if hasattr(config_module, "INFLUX_TOKEN"):
                        default_token = getattr(config_module, "INFLUX_TOKEN")
                        if default_token and not str(default_token).upper().startswith("REPLACE"):
                            token_pool.append(default_token)
                    if token_pool:
                        token = random.choice(token_pool)
                url = (chosen_profile or {}).get("url") or url or getattr(config_module, "INFLUX_URL", None)
                org = (chosen_profile or {}).get("org") or org or getattr(config_module, "INFLUX_ORG", None)
                bucket = (chosen_profile or {}).get("bucket") or bucket or getattr(config_module, "INFLUX_BUCKET", None)
            except ModuleNotFoundError:
                pass
        return InfluxConfig(
            url=url or "http://localhost:8086",
            token=token,
            org=org or "nof",
            bucket=bucket or "nof",
        )


class InfluxWriter:
    """Handles writing structured points into InfluxDB."""

    def __init__(self, config: InfluxConfig) -> None:
        if not config.token:
            raise ValueError("InfluxDB token is required. Set INFLUX_TOKEN environment variable.")
        self.config = config
        self._client = InfluxDBClient(
            url=config.url,
            token=config.token,
            org=config.org,
        )
        self._write_api = self._client.write_api()

    def close(self) -> None:
        self._write_api.__del__()  # type: ignore[attr-defined]
        self._client.close()

    def write_indicator_set(
        self,
        measurement: str,
        tags: Dict[str, str],
        fields: Dict[str, float | int | str],
        timestamp_ns: int,
    ) -> None:
        point = Point(measurement).tag("instrument_id", tags.get("instrument_id", "unknown"))
        for key, value in tags.items():
            point = point.tag(key, value)
        for field, value in fields.items():
            if value is None:
                continue
            point = point.field(field, value)
        point = point.time(timestamp_ns, WritePrecision.NS)
        self._write_api.write(bucket=self.config.bucket, org=self.config.org, record=point)

    def write_signal(
        self,
        *,
        instrument_id: str,
        model_id: str,
        decision: str,
        confidence: float,
        reasoning: str | None,
        order: Dict[str, Any] | None,
        generated_at: datetime,
    ) -> None:
        """Persist a single AI signal decision into InfluxDB."""
        if generated_at.tzinfo is None:
            generated_at = generated_at.replace(tzinfo=timezone.utc)
        timestamp_ns = int(generated_at.timestamp() * 1e9)
        point = (
            Point("ai_signals")
            .tag("instrument_id", instrument_id)
            .tag("model_id", model_id)
            .tag("decision", decision or "n/a")
            .field("confidence", float(confidence))
        )
        if reasoning:
            point = point.field("reasoning", reasoning)
        if order:
            order_json = json.dumps(order, ensure_ascii=False, sort_keys=True)
            point = point.field("order", order_json)
        point = point.time(timestamp_ns, WritePrecision.NS)
        self._write_api.write(bucket=self.config.bucket, org=self.config.org, record=point)

    def write_liquidation(
        self,
        *,
        instrument_id: str,
        timestamp: datetime,
        long_qty: float,
        short_qty: float,
        net_qty: float,
        price: float | None,
    ) -> None:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp_ns = int(timestamp.timestamp() * 1e9)
        point = (
            Point("okx_liquidations")
            .tag("instrument_id", instrument_id)
            .field("long_qty", float(long_qty))
            .field("short_qty", float(short_qty))
            .field("net_qty", float(net_qty))
        )
        if price is not None:
            point = point.field("last_price", float(price))
        point = point.time(timestamp_ns, WritePrecision.NS)
        self._write_api.write(bucket=self.config.bucket, org=self.config.org, record=point)

    def write_orderbook(
        self,
        *,
        instrument_id: str,
        timestamp: datetime,
        bids: Iterable[Iterable[float]],
        asks: Iterable[Iterable[float]],
    ) -> None:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp_ns = int(timestamp.timestamp() * 1e9)
        bids_list: List[List[float]] = [list(level) for level in bids]
        asks_list: List[List[float]] = [list(level) for level in asks]
        best_bid = bids_list[0][0] if bids_list else None
        best_ask = asks_list[0][0] if asks_list else None
        total_bid = sum(level[1] for level in bids_list) if bids_list else 0.0
        total_ask = sum(level[1] for level in asks_list) if asks_list else 0.0
        net_depth = total_bid - total_ask
        spread = None
        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid
        net_depth = total_bid - total_ask
        point = (
            Point("okx_orderbook_depth")
            .tag("instrument_id", instrument_id)
            .field("total_bid_qty", float(total_bid))
            .field("total_ask_qty", float(total_ask))
            .field("net_depth", float(net_depth))
        )
        if best_bid is not None:
            point = point.field("best_bid", float(best_bid))
        if best_ask is not None:
            point = point.field("best_ask", float(best_ask))
        if spread is not None:
            point = point.field("spread", float(spread))
        point = point.field("bids_json", json.dumps(bids_list, ensure_ascii=False))
        point = point.field("asks_json", json.dumps(asks_list, ensure_ascii=False))
        point = point.time(timestamp_ns, WritePrecision.NS)
        self._write_api.write(bucket=self.config.bucket, org=self.config.org, record=point)

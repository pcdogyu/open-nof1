"""
InfluxDB writer utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
import json
import os
import threading

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.exceptions import InfluxDBError


STATE_DIR = Path(__file__).resolve().parent.parent / "data" / "state"
PROFILE_CACHE_FILE = STATE_DIR / "active_influx_profile.json"
_PROFILE_CACHE_LOCK = threading.Lock()
_PROFILE_MEMORY_CACHE: dict[str, str] | None = None


@dataclass(slots=True)
class InfluxConfig:
    """Configuration required to connect to InfluxDB."""

    url: str = "http://localhost:8086"
    token: str | None = None
    org: str = "nof"
    bucket: str = "nof"

    @staticmethod
    def from_env() -> "InfluxConfig":
        # Gather candidate tokens from env/config and pick the first reachable one.
        tokens_env = os.getenv("INFLUX_TOKENS")
        base_url = os.getenv("INFLUX_URL") or "http://localhost:8086"
        base_org = os.getenv("INFLUX_ORG") or "nof"
        base_bucket = os.getenv("INFLUX_BUCKET") or "nof"
        raw_token = os.getenv("INFLUX_TOKEN")

        try:
            import config as config_module  # type: ignore
        except ModuleNotFoundError:
            config_module = None  # type: ignore

        if config_module is not None:
            base_url = os.getenv("INFLUX_URL") or getattr(config_module, "INFLUX_URL", base_url)
            base_org = os.getenv("INFLUX_ORG") or getattr(config_module, "INFLUX_ORG", base_org)
            base_bucket = os.getenv("INFLUX_BUCKET") or getattr(config_module, "INFLUX_BUCKET", base_bucket)

        candidate_profiles: list[dict[str, str]] = []
        seen_profiles: set[tuple[str, str, str, str]] = set()

        def add_candidate(
            *,
            token_value: str | None,
            profile_data: dict[str, Any] | None = None,
        ) -> None:
            if not token_value:
                return
            if str(token_value).upper().startswith("REPLACE"):
                return
            profile_dict = profile_data or {}
            normalized = _normalize_profile(
                {
                    "url": profile_dict.get("url") or base_url,
                    "org": profile_dict.get("org") or base_org,
                    "bucket": profile_dict.get("bucket") or base_bucket,
                    "token": token_value,
                }
            )
            key = (
                normalized["url"],
                normalized["org"],
                normalized["bucket"],
                normalized["token"],
            )
            if key in seen_profiles:
                return
            seen_profiles.add(key)
            candidate_profiles.append(normalized)

        if tokens_env:
            for token_candidate in [
                t.strip()
                for t in tokens_env.split(",")
                if t.strip()
            ]:
                add_candidate(token_value=token_candidate)
        add_candidate(token_value=raw_token)

        if config_module is not None:
            if hasattr(config_module, "INFLUX_PROFILES"):
                raw_profiles = getattr(config_module, "INFLUX_PROFILES")
                if isinstance(raw_profiles, (list, tuple)):
                    for profile in raw_profiles:
                        if not isinstance(profile, dict):
                            continue
                        if not profile.get("enabled", True):
                            continue
                        add_candidate(token_value=profile.get("token"), profile_data=profile)
            if hasattr(config_module, "INFLUX_TOKENS"):
                raw_tokens = getattr(config_module, "INFLUX_TOKENS")
                if isinstance(raw_tokens, (list, tuple)):
                    for token_value in raw_tokens:
                        add_candidate(token_value=token_value)
            if hasattr(config_module, "INFLUX_TOKEN"):
                add_candidate(token_value=getattr(config_module, "INFLUX_TOKEN"))

        working_profile = _select_working_profile(candidate_profiles)
        if working_profile is None and candidate_profiles:
            working_profile = candidate_profiles[0]

        if working_profile is None:
            raise ValueError("InfluxDB token is required. Set INFLUX_TOKEN environment variable.")

        return InfluxConfig(
            url=working_profile["url"],
            token=working_profile["token"],
            org=working_profile["org"],
            bucket=working_profile["bucket"],
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


def _normalize_profile(profile: dict[str, Any]) -> dict[str, str]:
    return {
        "url": str(profile.get("url") or "http://localhost:8086"),
        "org": str(profile.get("org") or "nof"),
        "bucket": str(profile.get("bucket") or "nof"),
        "token": str(profile.get("token")),
    }


def _profiles_equal(a: dict[str, str], b: dict[str, str]) -> bool:
    return (
        a.get("url") == b.get("url")
        and a.get("org") == b.get("org")
        and a.get("bucket") == b.get("bucket")
        and a.get("token") == b.get("token")
    )


def _select_working_profile(candidates: list[dict[str, str]]) -> dict[str, str] | None:
    prioritized = _prioritize_candidates(candidates)
    for profile in prioritized:
        if _test_influx_profile(profile):
            _save_cached_profile(profile)
            return profile
    return None


def _prioritize_candidates(candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    cached = _load_cached_profile()
    if not cached:
        return candidates
    normalized_cached = _normalize_profile(cached)
    for idx, profile in enumerate(candidates):
        if _profiles_equal(profile, normalized_cached):
            return [profile] + candidates[:idx] + candidates[idx + 1 :]
    return [normalized_cached] + candidates


def _test_influx_profile(profile: dict[str, str]) -> bool:
    client: InfluxDBClient | None = None
    try:
        client = InfluxDBClient(
            url=profile["url"],
            token=profile["token"],
            org=profile["org"],
        )
        buckets_api = client.buckets_api()
        try:
            buckets_api.find_bucket_by_name(profile["bucket"])
            return True
        except InfluxDBError as exc:
            status_code = getattr(exc, "status_code", None)
            if status_code is None and hasattr(exc, "response"):
                status_code = getattr(exc.response, "status", None)  # type: ignore[attr-defined]
            if status_code in (401, 403):
                return False
            if status_code == 404:
                return True  # Bucket missing, but auth works.
            raise
    except InfluxDBError:
        return False
    except Exception:
        return False
    finally:
        if client is not None:
            client.close()


def _load_cached_profile() -> dict[str, str] | None:
    global _PROFILE_MEMORY_CACHE
    with _PROFILE_CACHE_LOCK:
        if _PROFILE_MEMORY_CACHE is not None:
            return _PROFILE_MEMORY_CACHE
        try:
            with PROFILE_CACHE_FILE.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            return None
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        if not data.get("token"):
            return None
        _PROFILE_MEMORY_CACHE = {
            "url": str(data.get("url") or "http://localhost:8086"),
            "org": str(data.get("org") or "nof"),
            "bucket": str(data.get("bucket") or "nof"),
            "token": str(data["token"]),
        }
        return _PROFILE_MEMORY_CACHE


def _save_cached_profile(profile: dict[str, str]) -> None:
    normalized = _normalize_profile(profile)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with _PROFILE_CACHE_LOCK:
        with PROFILE_CACHE_FILE.open("w", encoding="utf-8") as handle:
            json.dump(normalized, handle, ensure_ascii=False, indent=2)
        global _PROFILE_MEMORY_CACHE
        _PROFILE_MEMORY_CACHE = normalized

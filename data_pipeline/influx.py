"""
InfluxDB writer utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict
import os

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
        if not token:
            try:  # fallback to config.py
                from config import INFLUX_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET  # type: ignore

                token = INFLUX_TOKEN
                url = url or INFLUX_URL
                org = org or INFLUX_ORG
                bucket = bucket or INFLUX_BUCKET
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

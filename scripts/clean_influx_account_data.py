"""
Clean problematic InfluxDB points for account_* measurements.

This script reads Influx connection info from environment variables first,
and falls back to values in config.py via InfluxConfig.from_env().

Two cleanup strategies are provided:

1) fields (default):
   Delete all points for numeric fields that are commonly contaminated by
   string writes. This is the safest generic fix when schema collisions
   (string vs float) break pivot queries.

2) measurement:
   Delete entire measurements (account_*). Use only when you want a clean
   slate and will backfill from your sync jobs.

Example (PowerShell):
  python -m scripts.clean_influx_account_data --dry-run
  python -m scripts.clean_influx_account_data --start 2020-01-01T00:00:00Z --stop 2100-01-01T00:00:00Z

"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, List

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from data_pipeline.influx import InfluxConfig


# Measurements and their numeric fields that should be safe to delete and rebuild
NUMERIC_FIELDS_BY_MEASUREMENT: Dict[str, List[str]] = {
    "account_snapshots": [
        "starting_equity",
        "cash_balance",
        "equity",
        "pnl",
        "created_at_ns",
        # base_currency is string — do NOT include
    ],
    "account_positions": [
        "quantity",
        "entry_price",
        "mark_price",
        "leverage",
        "unrealized_pnl",
    ],
    "account_trades": [
        "quantity",
        "price",
        "fee",
        "realized_pnl",
    ],
    "account_equity_curve": [
        "equity",
    ],
    "account_balances": [
        "total",
        "available",
        "frozen",
        "equity",
    ],
    "account_orders": [
        "size",
        "filled_size",
        "price",
        "average_price",
        # state/order_type/side are strings — do NOT include
    ],
}


def build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean InfluxDB account_* data")
    parser.add_argument(
        "--mode",
        choices=["fields", "measurement"],
        default="fields",
        help="Cleanup strategy: delete numeric fields only (default) or entire measurements",
    )
    parser.add_argument(
        "--start",
        default="2020-01-01T00:00:00Z",
        help="RFC3339 start time (inclusive) for deletion",
    )
    parser.add_argument(
        "--stop",
        default="2100-01-01T00:00:00Z",
        help="RFC3339 stop time (exclusive) for deletion",
    )
    parser.add_argument("--bucket", default=None, help="Override bucket name")
    parser.add_argument("--org", default=None, help="Override org name")
    parser.add_argument("--url", default=None, help="Override Influx URL")
    parser.add_argument("--token", default=None, help="Override Influx token")
    parser.add_argument("--dry-run", action="store_true", help="Print actions only")
    return parser.parse_args()


def main() -> int:
    args = build_args()

    config = InfluxConfig.from_env()
    if args.bucket:
        config.bucket = args.bucket
    if args.org:
        config.org = args.org
    if args.url:
        config.url = args.url
    if args.token:
        config.token = args.token

    if not config.token:
        print("ERROR: Influx token is missing. Set env vars or config.py.", file=sys.stderr)
        return 2

    client = InfluxDBClient(url=config.url, token=config.token, org=config.org)
    # Touch write_api to validate credentials early
    client.write_api(write_options=SYNCHRONOUS)
    delete_api = client.delete_api()

    start = args.start
    stop = args.stop

    if args.mode == "measurement":
        to_delete = list(NUMERIC_FIELDS_BY_MEASUREMENT.keys())
        for measurement in to_delete:
            predicate = f'_measurement="{measurement}"'
            print(f"[delete] bucket={config.bucket} measurement={measurement} predicate={predicate}")
            if not args.dry_run:
                delete_api.delete(
                    start=start,
                    stop=stop,
                    predicate=predicate,
                    bucket=config.bucket,
                    org=config.org,
                )
        print("Done.")
        return 0

    # Default: fields mode — delete only numeric fields which may have mixed types
    for measurement, fields in NUMERIC_FIELDS_BY_MEASUREMENT.items():
        for field in fields:
            predicate = f'_measurement="{measurement}" AND _field="{field}"'
            print(f"[delete] bucket={config.bucket} measurement={measurement} field={field} predicate={predicate}")
            if not args.dry_run:
                delete_api.delete(
                    start=start,
                    stop=stop,
                    predicate=predicate,
                    bucket=config.bucket,
                    org=config.org,
                )
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



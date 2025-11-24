"""
Command-line helper for placing and cancelling OKX demo orders.

Usage examples:
    python scripts/okx_demo_trade.py place \
        --inst-id BTC-USDT-SWAP --side buy --type limit --size 0.001 --price 24000

    python scripts/okx_demo_trade.py cancel \
        --inst-id BTC-USDT-SWAP --order-id 123456789012345678

Environment variables:
    OKX_API_KEY
    OKX_API_SECRET
    OKX_API_PASSPHRASE
    OKX_SIMULATE (optional, defaults to "1" for demo trading)
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from exchanges.base_client import ExchangeCredentials
from exchanges.okx.paper import OkxPaperClient, OkxClientError


def main() -> None:
    parser = argparse.ArgumentParser(description="OKX Demo Trade Helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    place_parser = subparsers.add_parser("place", help="Submit an order to OKX")
    place_parser.add_argument("--inst-id", required=True, help="OKX instrument ID")
    place_parser.add_argument(
        "--side", required=True, choices=["buy", "sell"], help="Order side"
    )
    place_parser.add_argument(
        "--type", required=True, choices=["limit", "market"], help="Order type"
    )
    place_parser.add_argument("--size", required=True, help="Order size (OKX sz)")
    place_parser.add_argument("--price", help="Limit price (required for limit)")
    place_parser.add_argument("--client-order-id", help="Optional client order id")
    place_parser.add_argument(
        "--margin-mode",
        default="cash",
        choices=["cash", "cross", "isolated"],
        help="Trading mode",
    )
    place_parser.add_argument(
        "--lever",
        type=float,
        default=2.0,
        help="Order leverage (defaults to 2x)",
    )

    cancel_parser = subparsers.add_parser("cancel", help="Cancel an OKX order")
    cancel_parser.add_argument("--inst-id", required=True, help="OKX instrument ID")
    cancel_parser.add_argument("--order-id", required=True, help="OKX order id")

    args = parser.parse_args()

    credentials = ExchangeCredentials(
        api_key=_env_or_exit("OKX_API_KEY"),
        api_secret=_env_or_exit("OKX_API_SECRET"),
        passphrase=os.environ.get("OKX_API_PASSPHRASE"),
    )

    simulate = os.environ.get("OKX_SIMULATE", "1") != "0"
    client = OkxPaperClient(simulate=simulate)
    try:
        client.authenticate(credentials)
        if args.command == "place":
            payload = {
                "instrument_id": args.inst_id,
                "side": args.side,
                "order_type": args.type,
                "size": args.size,
                "price": args.price,
                "margin_mode": args.margin_mode,
                "client_order_id": args.client_order_id,
                "leverage": args.lever,
            }
            response = client.place_order(payload)
        else:  # cancel
            response = client.cancel_order(args.order_id, instrument_id=args.inst_id)
    except (OkxClientError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()

    print(json.dumps(response, indent=2))


def _env_or_exit(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Environment variable {name} is required", file=sys.stderr)
        sys.exit(2)
    return value


if __name__ == "__main__":
    main()

"""
Backfill AI signal decisions from the JSONL log into InfluxDB.

Usage:
    python scripts/backfill_ai_signals.py [--log-path data/logs/ai_signals.jsonl]
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_pipeline.influx import InfluxConfig, InfluxWriter
from data_pipeline.pipeline import SIGNAL_LOG_MAX_LINES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill AI signals into InfluxDB.")
    parser.add_argument(
        "--log-path",
        type=Path,
        default=Path("data/logs/ai_signals.jsonl"),
        help="Path to the AI signals JSONL log.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=SIGNAL_LOG_MAX_LINES,
        help="Number of most recent log entries to retain after backfilling.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse the log and show stats without writing to InfluxDB.",
    )
    return parser.parse_args()


def _parse_timestamp(value: str) -> datetime:
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def backfill(log_path: Path, writer: InfluxWriter | None, *, dry_run: bool = False) -> int:
    if not log_path.exists():
        raise FileNotFoundError(f"Signal log not found: {log_path}")

    stored = 0
    with log_path.open("r", encoding="utf-8") as fp:
        for line_number, raw in enumerate(fp, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Malformed JSON on line {line_number}: {exc}") from exc

            timestamp = _parse_timestamp(payload["timestamp"])
            if dry_run:
                stored += 1
                continue
            if writer is None:
                raise RuntimeError("Influx writer is not available for backfilling.")

            writer.write_signal(
                instrument_id=payload["instrument_id"],
                model_id=payload["model_id"],
                decision=payload.get("decision", ""),
                confidence=float(payload.get("confidence", 0.0)),
                reasoning=payload.get("reasoning"),
                order=payload.get("order"),
                generated_at=timestamp,
            )
            stored += 1
    return stored


def truncate_log(path: Path, max_lines: int) -> int:
    if max_lines <= 0 or not path.exists():
        return 0

    from collections import deque

    buffer = deque(maxlen=max_lines)
    total = 0
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            buffer.append(line)
            total += 1
    if total <= max_lines:
        return total
    with path.open("w", encoding="utf-8") as fp:
        fp.writelines(buffer)
    return max_lines


def main() -> None:
    args = parse_args()
    config = InfluxConfig.from_env()
    writer: InfluxWriter | None = None

    try:
        if not args.dry_run:
            writer = InfluxWriter(config)
        stored = backfill(args.log_path, writer, dry_run=args.dry_run)
        retained = truncate_log(args.log_path, args.limit)
    finally:
        if writer is not None:
            writer.close()

    print(f"Processed {stored} AI signals.")
    if args.limit > 0:
        print(f"Log truncated to the latest {retained} entries.")


if __name__ == "__main__":
    main()

"""Backfill 2-year history in bounded batches."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data import backfill_history_batch  # noqa: E402

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill 2-year history in batches")
    parser.add_argument("--market", choices=["a", "hk", "us", "all"], default="all")
    parser.add_argument("--trade-date", default=None, help="Bootstrap state if missing, YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--pause-seconds", type=float, default=0.0)
    parser.add_argument("--pause-jitter-seconds", type=float, default=0.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-pause-seconds", type=float, default=0.5)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    markets = ["a", "hk", "us"] if args.market == "all" else [args.market]
    for market in markets:
        result = backfill_history_batch(
            market,
            batch_size=args.batch_size,
            trade_date=args.trade_date,
            continue_on_error=args.continue_on_error,
            pause_seconds=args.pause_seconds,
            pause_jitter_seconds=args.pause_jitter_seconds,
            retries=args.retries,
            retry_pause_seconds=args.retry_pause_seconds,
        )
        logger.info("history batch finished: %s", result)
        print(
            f"{market}: latest_trade_date={result['latest_trade_date']} "
            f"success={result['success']} errors={result['errors']} remaining={result['remaining']} "
            f"complete={result['history_complete']}"
        )


if __name__ == "__main__":
    main()

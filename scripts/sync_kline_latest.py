"""Sync latest-date daily files from flattened snapshots."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data import sync_latest_market_data  # noqa: E402

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync latest-date full-market daily data")
    parser.add_argument("--market", choices=["a", "hk", "us", "all"], default="all")
    parser.add_argument("--trade-date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    markets = ["a", "hk", "us"] if args.market == "all" else [args.market]
    for market in markets:
        result = sync_latest_market_data(market, trade_date=args.trade_date)
        logger.info("latest daily synced: %s", result)
        print(
            f"{market}: trade_date={result['trade_date']} rows={result['rows']} "
            f"symbols={result['symbols']} latest={result['latest_daily_path']}"
        )


if __name__ == "__main__":
    main()

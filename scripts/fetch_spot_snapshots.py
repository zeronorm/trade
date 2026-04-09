"""Fetch and store flattened spot snapshots."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data import fetch_and_store_snapshot  # noqa: E402

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch flattened market snapshots")
    parser.add_argument("--market", choices=["a", "hk", "us", "all"], default="all")
    parser.add_argument("--trade-date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    markets = ["a", "hk", "us"] if args.market == "all" else [args.market]
    for market in markets:
        frame, path = fetch_and_store_snapshot(market, trade_date=args.trade_date)
        logger.info("snapshot saved: market=%s rows=%s path=%s", market, len(frame), path)
        print(f"{market}: rows={len(frame)} path={path}")


if __name__ == "__main__":
    main()

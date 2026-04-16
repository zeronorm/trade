"""Sync per-market day spot and symbol history in one execution window."""

from __future__ import annotations

import argparse
from datetime import datetime
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data import sync_market_data  # noqa: E402

logger = logging.getLogger(__name__)

MARKET_TIMEZONES = {
    "cn": "Asia/Shanghai",
    "hk": "Asia/Shanghai",
    "us": "America/New_York",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync per-market day spot and symbol history")
    parser.add_argument("--market", choices=["cn", "hk", "us", "all"], default="all")
    parser.add_argument(
        "--trade-date",
        default=None,
        help="YYYY-MM-DD or YYYYMMDD; defaults to the current date in each market timezone",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--adjust", default="qfq")
    parser.add_argument("--continue-on-error", action="store_true")
    return parser.parse_args()


def resolve_trade_date(market: str, trade_date: str | None) -> str:
    if trade_date:
        return trade_date
    timezone = MARKET_TIMEZONES[market]
    return datetime.now(ZoneInfo(timezone)).strftime("%Y-%m-%d")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    markets = ["cn", "hk", "us"] if args.market == "all" else [args.market]
    for market in markets:
        trade_date = resolve_trade_date(market, args.trade_date)
        result = sync_market_data(
            market,
            trade_date=trade_date,
            limit=args.limit,
            adjust=args.adjust,
            continue_on_error=args.continue_on_error,
        )
        logger.info("market data synced: %s", result)
        interrupted = " interrupted=true" if result.get("interrupted") else ""
        print(
            f"{market}: trade_date={result['trade_date']} "
            f"day_rows={result['day_rows']} "
            f"day_api_time_field={result['day_api_time_field']} day_api_time_value={result['day_api_time_value']} "
            f"day={result['day_path']} "
            f"new_symbols={result['hist_new_symbols']} success={result['hist_success']} "
            f"skipped={result['hist_skipped']} errors={result['hist_errors']} "
            f"hist_sample_trade_date={result['hist_sample_trade_date']} hist={result['hist_path']}{interrupted}"
        )


if __name__ == "__main__":
    main()

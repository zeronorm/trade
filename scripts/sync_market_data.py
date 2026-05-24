"""Sync per-market day spot and symbol history in one execution window."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data import sync_market_data  # noqa: E402

MARKET_TZ = {"cn": "Asia/Shanghai", "hk": "Asia/Shanghai", "us": "America/New_York"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync market data")
    p.add_argument("--market", choices=["cn", "hk", "us", "all"], default="all")
    p.add_argument("--trade-date", default=None, help="YYYY-MM-DD or YYYYMMDD")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--adjust", default="qfq")
    p.add_argument("--hist-retries", type=int, default=3, help="retry times per symbol history fetch")
    p.add_argument("--hist-retry-delay", type=float, default=5.0, help="seconds between history retries")
    p.add_argument("--fail-fast", action="store_true", help="stop immediately when a single symbol fails")
    p.add_argument("--no-merge", action="store_true", help="skip merge step (useful on ecs)")
    return p.parse_args()


def resolve_date(market: str, trade_date: str | None) -> str:
    if trade_date:
        return trade_date
    return datetime.now(ZoneInfo(MARKET_TZ[market])).strftime("%Y-%m-%d")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    markets = ["cn", "hk", "us"] if args.market == "all" else [args.market]

    for mkt in markets:
        td = resolve_date(mkt, args.trade_date)
        r = sync_market_data(
            mkt, trade_date=td, limit=args.limit,
            adjust=args.adjust,
            continue_on_error=not args.fail_fast,
            hist_retries=args.hist_retries,
            hist_retry_delay=args.hist_retry_delay,
            merge=not args.no_merge,
        )
        print(
            f"{mkt}: date={r['trade_date']} "
            f"day={r['day_rows']}rows → {r['day_path']} "
            f"hist: total={r['hist_total_symbols']} pending={r['symbols_to_fetch']} ok={r['hist_success']} "
            f"err={r['hist_errors']} cached={r['hist_cached']} → {r['hist_path']} "
            f"merge → {r['merge_path']}"
        )


if __name__ == "__main__":
    main()

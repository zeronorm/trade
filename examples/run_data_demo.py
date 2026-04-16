"""Minimal example for the unified market data sync process."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data import sync_market_data  # noqa: E402


def main() -> None:
    trade_date = "2026-04-10"
    market = "cn"

    result = sync_market_data(market, trade_date=trade_date, limit=3, continue_on_error=True)
    print(f"[process] market_data result={result}")


if __name__ == "__main__":
    main()

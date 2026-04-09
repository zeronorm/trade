"""Small demo for data-layer entrypoints."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data import fetch_market_snapshot  # noqa: E402


def main() -> None:
    for market in ["a", "hk", "us"]:
        frame = fetch_market_snapshot(market)
        print(f"\n[{market}] rows={len(frame)}")
        print(frame.head(3).to_string(index=False))


if __name__ == "__main__":
    main()

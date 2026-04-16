import pandas as pd

from src.data.storage import SymbolHistStore


def test_symbol_hist_store_merge_and_exists(tmp_path) -> None:
    store = SymbolHistStore(tmp_path / "hist")
    frame1 = pd.DataFrame(
        [
            {"market": "us", "board": "", "symbol": "UFCS", "trade_date": "2026-04-10"},
            {"market": "us", "board": "", "symbol": "AAPL", "trade_date": "2026-04-10"},
        ]
    )
    frame2 = pd.DataFrame(
        [
            {"market": "us", "board": "", "symbol": "UFCS", "trade_date": "2026-04-10"},
            {"market": "us", "board": "", "symbol": "MSFT", "trade_date": "2026-04-11"},
        ]
    )

    store.save(frame1, "us")
    store.save(frame2, "us")

    merged, _ = store.load("us")
    assert store.has_symbol_trade_date("us", "", "UFCS", "2026-04-10") is True
    assert store.has_symbol_trade_date("us", "", "MSFT", "2026-04-11") is True
    assert len(merged) == 3

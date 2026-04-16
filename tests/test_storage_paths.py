import pandas as pd

from src.data.storage import MarketDaySpotStore, SymbolHistStore


def test_storage_paths(tmp_path) -> None:
    day_store = MarketDaySpotStore(tmp_path / "day")
    hist_store = SymbolHistStore(tmp_path / "hist")

    assert day_store.build_path("cn", "2026-04-09").name == "cn.2026-04-09.csv"
    assert hist_store.build_path("hk").name == "hk.csv"
    assert hist_store.build_path("cn").name == "cn.csv"


def test_day_store_preserves_leading_zero_symbols(tmp_path) -> None:
    day_store = MarketDaySpotStore(tmp_path / "day")
    sample = pd.DataFrame(
        [{"market": "hk", "board": "", "symbol": "00542", "provider_symbol": "00542", "trade_date": "2026-04-12"}]
    )
    day_store.save(sample, "hk", "2026-04-12")
    loaded, _ = day_store.load("hk", "2026-04-12")
    assert loaded.loc[0, "symbol"] == "00542"
    assert loaded.loc[0, "provider_symbol"] == "00542"

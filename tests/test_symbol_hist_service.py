import pandas as pd

from src.data.services.symbol_hist_service import SymbolHistService


class DummySpotService:
    def load_market_day_spot(self, market: str, *, trade_date: str):
        current = pd.DataFrame(
            [
                {"board": "sh", "symbol": "600000", "provider_symbol": "600000"},
                {"board": "sz", "symbol": "000001", "provider_symbol": "000001"},
            ]
        )
        return current, f"{market}.{trade_date}.csv"

    def load_previous_market_day_spot(self, market: str, *, trade_date: str):
        previous = pd.DataFrame(
            [
                {"board": "sh", "symbol": "600000", "provider_symbol": "600000"},
            ]
        )
        return previous, f"{market}.prev.csv"


def test_diff_new_symbols() -> None:
    service = SymbolHistService(spot_service=DummySpotService())  # type: ignore[arg-type]
    frame, current_path, previous_path = service.diff_new_symbols("cn", trade_date="2026-04-09")

    assert current_path == "cn.2026-04-09.csv"
    assert previous_path == "cn.prev.csv"
    assert frame[["board", "symbol"]].to_dict("records") == [{"board": "sz", "symbol": "000001"}]

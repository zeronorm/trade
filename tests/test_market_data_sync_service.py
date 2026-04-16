from src.data.services.market_data_sync_service import MarketDataSyncService


class DummySpotService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def store_market_day_spot(self, market: str, *, trade_date: str):
        class DummyFrame(list):
            attrs = {"sample_api_time_field": "时间戳", "sample_api_time_value": "15:30:00"}

        self.calls.append(("spot", market, trade_date))
        return DummyFrame([1, 2, 3]), f"day/{market}.{trade_date}.csv"


class DummyHistService:
    def __init__(self, calls: list[tuple[str, str, str]]) -> None:
        self.calls = calls

    def sync_new_symbol_hist(
        self,
        market: str,
        *,
        trade_date: str,
        limit: int | None = None,
        adjust: str = "qfq",
        continue_on_error: bool = True,
    ):
        self.calls.append(("hist", market, trade_date))
        return {
            "market": market,
            "trade_date": trade_date,
            "new_symbols": limit or 0,
            "success": limit or 0,
            "skipped": 0,
            "errors": 0,
            "sample_trade_date": None,
            "hist_path": f"hist/{market}.csv",
            "interrupted": False,
        }


def test_sync_market_data_runs_spot_then_hist() -> None:
    spot_service = DummySpotService()
    hist_service = DummyHistService(spot_service.calls)
    service = MarketDataSyncService(spot_service=spot_service, hist_service=hist_service)  # type: ignore[arg-type]

    result = service.sync_market_data("cn", trade_date="20260409", limit=2)

    assert spot_service.calls == [("spot", "cn", "2026-04-09"), ("hist", "cn", "2026-04-09")]
    assert result == {
        "market": "cn",
        "trade_date": "2026-04-09",
        "day_rows": 3,
        "day_api_time_field": "时间戳",
        "day_api_time_value": "15:30:00",
        "day_path": "day/cn.2026-04-09.csv",
        "hist_new_symbols": 2,
        "hist_success": 2,
        "hist_skipped": 0,
        "hist_errors": 0,
        "hist_sample_trade_date": None,
        "hist_path": "hist/cn.csv",
        "interrupted": False,
    }

from src.data.models import DAILY_COLUMNS, SNAPSHOT_COLUMNS, history_start_date, normalize_trade_date, to_yyyymmdd


def test_date_helpers() -> None:
    assert normalize_trade_date("20260409") == "2026-04-09"
    assert to_yyyymmdd("2026-04-09") == "20260409"
    assert history_start_date("2026-04-09") == "20240409"


def test_flattened_columns() -> None:
    assert SNAPSHOT_COLUMNS == [
        "market",
        "symbol",
        "provider_symbol",
        "name",
        "open",
        "high",
        "low",
        "close",
        "prev_close",
        "volume",
        "amount",
        "turnover_rate",
        "source",
    ]
    assert DAILY_COLUMNS == [
        "market",
        "symbol",
        "provider_symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover_rate",
        "outstanding_share",
        "source",
    ]

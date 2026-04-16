from src.data.models import (
    MARKET_DAY_SPOT_COLUMNS,
    SYMBOL_HIST_COLUMNS,
    build_hist_key,
    detect_a_board,
    history_start_date,
    normalize_display_symbol,
    normalize_provider_symbol,
    normalize_trade_date,
    to_yyyymmdd,
)


def test_date_helpers() -> None:
    assert normalize_trade_date("20260409") == "2026-04-09"
    assert to_yyyymmdd("2026-04-09") == "20260409"
    assert history_start_date("2026-04-09") == "20240409"


def test_symbol_helpers() -> None:
    assert detect_a_board("600000") == "sh"
    assert detect_a_board("000001") == "sz"
    assert detect_a_board("bj920001") == "bj"
    assert normalize_display_symbol("cn", "sz000001") == "000001"
    assert normalize_display_symbol("hk", "700") == "00700"
    assert normalize_display_symbol("us", "105.MSFT") == "MSFT"
    assert normalize_provider_symbol("hk", "", "00700", "700") == "00700"
    assert build_hist_key("sh", "600000") == "sh.600000"
    assert build_hist_key("", "AAPL") == "AAPL"


def test_flattened_columns() -> None:
    assert MARKET_DAY_SPOT_COLUMNS == [
        "market",
        "board",
        "symbol",
        "provider_symbol",
        "name",
        "cname",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "trade_date",
        "source",
    ]
    assert SYMBOL_HIST_COLUMNS == [
        "market",
        "board",
        "symbol",
        "provider_symbol",
        "trade_date",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "change_pct",
        "change",
        "turnover_rate",
        "source",
    ]

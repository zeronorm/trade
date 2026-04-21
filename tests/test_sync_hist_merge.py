from __future__ import annotations

import pandas as pd

import src.data.fetch as fetch
import src.data.store as store
import src.data.sync as sync
from src.data.models import HIST_COLUMNS, SPOT_COLUMNS


def test_merge_hist_keeps_latest_row_per_symbol_and_trade_date(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "HIST_ROOT", tmp_path / "hist")

    frame_old = pd.DataFrame(
        [
            {
                "market": "cn",
                "board": "sh",
                "symbol": "600000",
                "provider_symbol": "sh600000",
                "trade_date": "2026-04-18",
                "open": 10,
                "close": 10,
                "high": 11,
                "low": 9,
                "volume": 100,
                "amount": 1000,
                "amplitude": pd.NA,
                "change_pct": pd.NA,
                "change": pd.NA,
                "turnover_rate": pd.NA,
                "source": "old",
            },
            {
                "market": "cn",
                "board": "sz",
                "symbol": "000001",
                "provider_symbol": "sz000001",
                "trade_date": "2026-04-18",
                "open": 20,
                "close": 20,
                "high": 21,
                "low": 19,
                "volume": 200,
                "amount": 2000,
                "amplitude": pd.NA,
                "change_pct": pd.NA,
                "change": pd.NA,
                "turnover_rate": pd.NA,
                "source": "old",
            },
        ],
        columns=HIST_COLUMNS,
    )
    frame_new = pd.DataFrame(
        [
            {
                "market": "cn",
                "board": "sh",
                "symbol": "600000",
                "provider_symbol": "sh600000",
                "trade_date": "2026-04-18",
                "open": 10,
                "close": 12,
                "high": 12,
                "low": 9,
                "volume": 300,
                "amount": 3000,
                "amplitude": pd.NA,
                "change_pct": pd.NA,
                "change": pd.NA,
                "turnover_rate": pd.NA,
                "source": "new",
            },
            {
                "market": "cn",
                "board": "sh",
                "symbol": "600000",
                "provider_symbol": "sh600000",
                "trade_date": "2026-04-19",
                "open": 12,
                "close": 13,
                "high": 13,
                "low": 11,
                "volume": 400,
                "amount": 4000,
                "amplitude": pd.NA,
                "change_pct": pd.NA,
                "change": pd.NA,
                "turnover_rate": pd.NA,
                "source": "new",
            },
        ],
        columns=HIST_COLUMNS,
    )

    store.save_hist(frame_old, "cn", "2026-04-19")
    store.save_hist(frame_new, "cn", "2026-04-20")

    out = store.merge_hist("cn", "2026-04-20")
    merged = store.load_hist_merge("cn", "2026-04-20")

    assert out.name == "cn.merge.2026-04-20.csv"
    assert len(merged) == 3
    latest = merged[(merged["board"] == "sh") & (merged["symbol"] == "600000") & (merged["trade_date"] == "2026-04-18")]
    assert latest.iloc[0]["close"] == 12
    assert latest.iloc[0]["source"] == "new"


def test_sync_market_data_fetches_all_current_symbols_not_only_new(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "DAY_ROOT", tmp_path / "day")
    monkeypatch.setattr(store, "HIST_ROOT", tmp_path / "hist")

    prev_day = pd.DataFrame(
        [
            {
                "market": "cn",
                "board": "sh",
                "symbol": "600000",
                "provider_symbol": "sh600000",
                "name": "PF Bank",
                "cname": "浦发银行",
                "open": 10,
                "close": 10,
                "high": 10,
                "low": 10,
                "volume": 1,
                "amount": 1,
                "trade_date": "2026-04-19",
                "source": "prev",
            }
        ],
        columns=SPOT_COLUMNS,
    )
    store.save_day(prev_day, "cn", "2026-04-19")

    current_day = pd.DataFrame(
        [
            {
                "market": "cn",
                "board": "sz",
                "symbol": "000001",
                "provider_symbol": "sz000001",
                "name": "PAYH",
                "cname": "平安银行",
                "open": 11,
                "close": 11,
                "high": 11,
                "low": 11,
                "volume": 2,
                "amount": 2,
                "trade_date": "2026-04-20",
                "source": "spot",
            },
            {
                "market": "cn",
                "board": "sh",
                "symbol": "600000",
                "provider_symbol": "sh600000",
                "name": "PF Bank",
                "cname": "浦发银行",
                "open": 10,
                "close": 10,
                "high": 10,
                "low": 10,
                "volume": 1,
                "amount": 1,
                "trade_date": "2026-04-20",
                "source": "spot",
            },
        ],
        columns=SPOT_COLUMNS,
    )

    hist_calls: list[str] = []

    def fake_fetch_spot(market: str, trade_date: str) -> pd.DataFrame:
        assert market == "cn"
        assert trade_date == "2026-04-20"
        return current_day

    def fake_fetch_hist(
        market: str,
        board: str,
        sym: str,
        *,
        trade_date: str,
        provider_sym: str = "",
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        hist_calls.append(f"{board}.{sym}")
        return pd.DataFrame(
            [
                {
                    "market": market,
                    "board": board,
                    "symbol": sym,
                    "provider_symbol": provider_sym,
                    "trade_date": trade_date,
                    "open": 1,
                    "close": 1,
                    "high": 1,
                    "low": 1,
                    "volume": 1,
                    "amount": 1,
                    "amplitude": pd.NA,
                    "change_pct": pd.NA,
                    "change": pd.NA,
                    "turnover_rate": pd.NA,
                    "source": "hist",
                }
            ],
            columns=HIST_COLUMNS,
        )

    monkeypatch.setattr(sync, "fetch_spot", fake_fetch_spot)
    monkeypatch.setattr(sync, "fetch_hist", fake_fetch_hist)

    result = sync.sync_market_data("cn", trade_date="20260420")

    assert hist_calls == ["sh.600000", "sz.000001"]
    assert result["hist_total_symbols"] == 2
    assert result["symbols_to_fetch"] == 2
    assert result["hist_success"] == 2
    assert result["hist_errors"] == 0
    assert result["hist_cached"] is False
    assert result["hist_path"].endswith("data_store/hist/cn.2026-04-20.csv")
    assert result["merge_path"].endswith("data_store/hist/cn.merge.2026-04-20.csv")

    merged = store.load_hist_merge("cn", "2026-04-20")
    assert len(merged) == 2


def test_sync_market_data_resumes_from_hist_and_progress(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "DAY_ROOT", tmp_path / "day")
    monkeypatch.setattr(store, "HIST_ROOT", tmp_path / "hist")

    current_day = pd.DataFrame(
        [
            {
                "market": "cn",
                "board": "bj",
                "symbol": "920000",
                "provider_symbol": "bj920000",
                "name": "A",
                "cname": "A",
                "open": 1,
                "close": 1,
                "high": 1,
                "low": 1,
                "volume": 1,
                "amount": 1,
                "trade_date": "2026-04-20",
                "source": "spot",
            },
            {
                "market": "cn",
                "board": "sh",
                "symbol": "600000",
                "provider_symbol": "sh600000",
                "name": "B",
                "cname": "B",
                "open": 1,
                "close": 1,
                "high": 1,
                "low": 1,
                "volume": 1,
                "amount": 1,
                "trade_date": "2026-04-20",
                "source": "spot",
            },
            {
                "market": "cn",
                "board": "sz",
                "symbol": "000001",
                "provider_symbol": "sz000001",
                "name": "C",
                "cname": "C",
                "open": 1,
                "close": 1,
                "high": 1,
                "low": 1,
                "volume": 1,
                "amount": 1,
                "trade_date": "2026-04-20",
                "source": "spot",
            },
        ],
        columns=SPOT_COLUMNS,
    )

    existing_hist = pd.DataFrame(
        [
            {
                "market": "cn",
                "board": "sh",
                "symbol": "600000",
                "provider_symbol": "sh600000",
                "trade_date": "2026-04-20",
                "open": 1,
                "close": 1,
                "high": 1,
                "low": 1,
                "volume": 1,
                "amount": 1,
                "amplitude": pd.NA,
                "change_pct": pd.NA,
                "change": pd.NA,
                "turnover_rate": pd.NA,
                "source": "hist",
            }
        ],
        columns=HIST_COLUMNS,
    )
    store.save_hist(existing_hist, "cn", "2026-04-20")
    store.append_hist_progress(
        "cn",
        "2026-04-20",
        board="sz",
        symbol="000001",
        provider_symbol="sz000001",
        has_data=False,
    )

    hist_calls: list[str] = []

    def fake_fetch_spot(market: str, trade_date: str) -> pd.DataFrame:
        assert market == "cn"
        assert trade_date == "2026-04-20"
        return current_day

    def fake_fetch_hist(
        market: str,
        board: str,
        sym: str,
        *,
        trade_date: str,
        provider_sym: str = "",
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        hist_calls.append(f"{board}.{sym}")
        return pd.DataFrame(
            [
                {
                    "market": market,
                    "board": board,
                    "symbol": sym,
                    "provider_symbol": provider_sym,
                    "trade_date": trade_date,
                    "open": 2,
                    "close": 2,
                    "high": 2,
                    "low": 2,
                    "volume": 2,
                    "amount": 2,
                    "amplitude": pd.NA,
                    "change_pct": pd.NA,
                    "change": pd.NA,
                    "turnover_rate": pd.NA,
                    "source": "hist",
                }
            ],
            columns=HIST_COLUMNS,
        )

    monkeypatch.setattr(sync, "fetch_spot", fake_fetch_spot)
    monkeypatch.setattr(sync, "fetch_hist", fake_fetch_hist)

    result = sync.sync_market_data("cn", trade_date="20260420")

    assert hist_calls == ["bj.920000"]
    assert result["hist_total_symbols"] == 3
    assert result["symbols_to_fetch"] == 1
    assert result["hist_success"] == 1
    assert result["hist_errors"] == 0
    assert result["hist_cached"] is False

    hist_frame = store.load_hist("cn", "2026-04-20")
    assert set(zip(hist_frame["board"], hist_frame["symbol"])) == {
        ("bj", "920000"),
        ("sh", "600000"),
    }


def test_sync_market_data_retries_before_counting_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "DAY_ROOT", tmp_path / "day")
    monkeypatch.setattr(store, "HIST_ROOT", tmp_path / "hist")

    current_day = pd.DataFrame(
        [
            {
                "market": "cn",
                "board": "sh",
                "symbol": "600000",
                "provider_symbol": "sh600000",
                "name": "B",
                "cname": "B",
                "open": 1,
                "close": 1,
                "high": 1,
                "low": 1,
                "volume": 1,
                "amount": 1,
                "trade_date": "2026-04-20",
                "source": "spot",
            },
        ],
        columns=SPOT_COLUMNS,
    )

    attempts = {"count": 0}

    def fake_fetch_spot(market: str, trade_date: str) -> pd.DataFrame:
        return current_day

    def fake_fetch_hist(
        market: str,
        board: str,
        sym: str,
        *,
        trade_date: str,
        provider_sym: str = "",
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise TimeoutError("temporary timeout")
        return pd.DataFrame(
            [
                {
                    "market": market,
                    "board": board,
                    "symbol": sym,
                    "provider_symbol": provider_sym,
                    "trade_date": trade_date,
                    "open": 2,
                    "close": 2,
                    "high": 2,
                    "low": 2,
                    "volume": 2,
                    "amount": 2,
                    "amplitude": pd.NA,
                    "change_pct": pd.NA,
                    "change": pd.NA,
                    "turnover_rate": pd.NA,
                    "source": "hist",
                }
            ],
            columns=HIST_COLUMNS,
        )

    monkeypatch.setattr(sync, "fetch_spot", fake_fetch_spot)
    monkeypatch.setattr(sync, "fetch_hist", fake_fetch_hist)

    result = sync.sync_market_data(
        "cn",
        trade_date="20260420",
        hist_retries=2,
        hist_retry_delay=0,
    )

    assert attempts["count"] == 3
    assert result["hist_success"] == 1
    assert result["hist_errors"] == 0
    hist_frame = store.load_hist("cn", "2026-04-20")
    assert len(hist_frame) == 1


def test_fetch_hist_hk_returns_empty_when_upstream_has_no_date(monkeypatch) -> None:
    def fake_stock_hk_daily(symbol: str, adjust: str = "") -> pd.DataFrame:
        raise KeyError("date")

    monkeypatch.setattr(fetch.ak, "stock_hk_daily", fake_stock_hk_daily)

    frame = fetch.fetch_hist("hk", "", "02938", trade_date="2026-04-20", provider_sym="02938")

    assert frame.empty is True
    assert list(frame.columns) == HIST_COLUMNS

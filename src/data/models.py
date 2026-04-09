"""Data-layer models and shared constants."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

MarketName = Literal["a", "hk", "us"]
ProviderName = Literal["sina"]

SNAPSHOT_COLUMNS = [
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

DAILY_COLUMNS = [
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

NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "volume",
    "amount",
    "turnover_rate",
    "outstanding_share",
]


@dataclass(frozen=True)
class SnapshotRequest:
    market: MarketName
    provider: ProviderName = "sina"


@dataclass(frozen=True)
class HistoryRequest:
    market: MarketName
    symbol: str
    provider_symbol: str | None = None
    provider: ProviderName = "sina"
    end_date: str = ""
    start_date: str = ""
    adjust: str = ""


@dataclass(frozen=True)
class SyncState:
    market: MarketName
    provider: ProviderName = "sina"
    latest_trade_date: str = ""
    snapshot_path: str = ""
    latest_daily_path: str = ""
    history_target_start_date: str = ""
    total_symbols: int = 0
    completed_symbols: tuple[str, ...] = ()
    failed_symbols: tuple[str, ...] = ()
    history_complete: bool = False


def normalize_trade_date(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    datetime.strptime(value, "%Y-%m-%d")
    return value


def to_yyyymmdd(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return value
    return normalize_trade_date(value).replace("-", "")


def history_start_date(end_date: str) -> str:
    end_dt = datetime.strptime(normalize_trade_date(end_date), "%Y-%m-%d")
    return (end_dt - timedelta(days=730)).strftime("%Y%m%d")

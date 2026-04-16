"""Data-layer models and shared constants."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

MarketName = Literal["cn", "hk", "us"]
ProviderName = Literal["akshare"]

MARKET_DAY_SPOT_COLUMNS = [
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

SYMBOL_HIST_COLUMNS = [
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

NUMERIC_COLUMNS = [
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
]


@dataclass(frozen=True)
class MarketDaySpotRequest:
    market: MarketName
    trade_date: str
    provider: ProviderName = "akshare"


@dataclass(frozen=True)
class SymbolHistRequest:
    market: MarketName
    board: str
    symbol: str
    trade_date: str
    provider_symbol: str = ""
    start_date: str = ""
    end_date: str = ""
    adjust: str = "qfq"
    provider: ProviderName = "akshare"


def normalize_trade_date(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    datetime.strptime(value, "%Y-%m-%d")
    return value


def to_yyyymmdd(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return value
    return normalize_trade_date(value).replace("-", "")


def history_start_date(end_date: str, *, days: int = 730) -> str:
    end_dt = datetime.strptime(normalize_trade_date(end_date), "%Y-%m-%d")
    return (end_dt - timedelta(days=days)).strftime("%Y%m%d")


def normalize_symbol(value: object) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def normalize_optional_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if text.lower() in {"nan", "none", "<na>"}:
        return ""
    return text


def detect_a_board(symbol: str) -> str:
    code = normalize_symbol(symbol).lower()
    if code.startswith(("sh", "sz", "bj")):
        return code[:2]
    if code.startswith(("4", "8", "92")):
        return "bj"
    if code.startswith(("5", "6", "9")):
        return "sh"
    return "sz"


def strip_a_prefix(symbol: str) -> str:
    code = normalize_symbol(symbol).lower()
    if code.startswith(("sh", "sz", "bj")):
        return code[2:]
    return code


def normalize_board(market: MarketName, symbol: object) -> str:
    text = normalize_symbol(symbol)
    if market == "cn":
        return detect_a_board(text)
    return ""


def normalize_display_symbol(market: MarketName, symbol: object) -> str:
    text = normalize_symbol(symbol)
    if market == "cn":
        return strip_a_prefix(text)
    if market == "hk":
        return text.zfill(5)
    if "." in text:
        return text.split(".")[-1]
    return text


def normalize_provider_symbol(market: MarketName, board: object, symbol: object, provider_symbol: object) -> str:
    current = normalize_optional_text(provider_symbol)
    if market == "cn":
        if current:
            return current
        normalized_board = normalize_optional_text(board)
        normalized_symbol = normalize_display_symbol("cn", symbol)
        return f"{normalized_board}{normalized_symbol}" if normalized_board else normalized_symbol
    if market == "hk":
        candidate = current or normalize_optional_text(symbol)
        return candidate.zfill(5)
    return current or normalize_optional_text(symbol)


def build_hist_key(board: str, symbol: str) -> str:
    normalized_board = normalize_optional_text(board)
    normalized_symbol = normalize_optional_text(symbol)
    return f"{normalized_board}.{normalized_symbol}" if normalized_board else normalized_symbol

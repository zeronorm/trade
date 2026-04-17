"""Shared types and normalization helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

MarketName = Literal["cn", "hk", "us"]

SPOT_COLUMNS = [
    "market", "board", "symbol", "provider_symbol",
    "name", "cname",
    "open", "close", "high", "low", "volume", "amount",
    "trade_date", "source",
]

HIST_COLUMNS = [
    "market", "board", "symbol", "provider_symbol", "trade_date",
    "open", "close", "high", "low", "volume", "amount",
    "amplitude", "change_pct", "change", "turnover_rate", "source",
]


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


def clean(value: object) -> str:
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


def detect_board(symbol: str) -> str:
    code = clean(symbol).lower()
    if code.startswith(("sh", "sz", "bj")):
        return code[:2]
    if code.startswith(("4", "8", "92")):
        return "bj"
    if code.startswith(("5", "6", "9")):
        return "sh"
    return "sz"


def strip_prefix(symbol: str) -> str:
    code = clean(symbol).lower()
    if code.startswith(("sh", "sz", "bj")):
        return code[2:]
    return code


def display_symbol(market: str, symbol: object) -> str:
    text = clean(symbol)
    if market == "cn":
        return strip_prefix(text)
    if market == "hk":
        return text.zfill(5)
    if "." in text:
        return text.split(".")[-1]
    return text


def provider_symbol(market: str, board: str, symbol: str, raw: object = "") -> str:
    current = clean(raw)
    if market == "cn":
        if current:
            return current
        b = clean(board)
        s = display_symbol("cn", symbol)
        return f"{b}{s}" if b else s
    if market == "hk":
        return (current or clean(symbol)).zfill(5)
    return current or clean(symbol)

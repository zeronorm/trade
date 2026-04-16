"""Akshare-based market day spot provider."""

from __future__ import annotations

import akshare as ak
import pandas as pd

from src.data.models import (
    MARKET_DAY_SPOT_COLUMNS,
    MarketDaySpotRequest,
    MarketName,
    normalize_board,
    normalize_display_symbol,
    normalize_symbol,
)
from src.data.providers.base import DataProviderError, MarketDaySpotProvider


def _pick_series(frame: pd.DataFrame, aliases: list[str], default=pd.NA) -> pd.Series:
    for alias in aliases:
        if alias in frame.columns:
            return frame[alias]
    return pd.Series([default] * len(frame), index=frame.index)


def _attach_api_time_attrs(normalized: pd.DataFrame, raw: pd.DataFrame, aliases: list[str]) -> pd.DataFrame:
    field = next((alias for alias in aliases if alias in raw.columns), None)
    value = None
    if field is not None and not raw.empty:
        value = str(raw.iloc[0].get(field, ""))
    normalized.attrs["sample_api_time_field"] = field
    normalized.attrs["sample_api_time_value"] = value
    return normalized


def _normalize_cn(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    raw_code = _pick_series(frame, ["代码"]).astype(str)
    board = raw_code.map(lambda value: normalize_board("cn", value))
    symbol = raw_code.map(lambda value: normalize_display_symbol("cn", value))
    normalized = pd.DataFrame(
        {
            "market": "cn",
            "board": board,
            "symbol": symbol,
            "provider_symbol": board.str.cat(symbol),
            "name": _pick_series(frame, ["名称"]).astype(str),
            "cname": _pick_series(frame, ["名称"]).astype(str),
            "open": _pick_series(frame, ["今开"]),
            "close": _pick_series(frame, ["最新价"]),
            "high": _pick_series(frame, ["最高"]),
            "low": _pick_series(frame, ["最低"]),
            "volume": _pick_series(frame, ["成交量"]),
            "amount": _pick_series(frame, ["成交额"]),
            "trade_date": trade_date,
            "source": "akshare.stock_zh_a_spot",
        }
    )
    return _attach_api_time_attrs(normalized, frame, ["时间戳"])


def _normalize_hk(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    raw_code = _pick_series(frame, ["代码"]).astype(str)
    normalized = pd.DataFrame(
        {
            "market": "hk",
            "board": "",
            "symbol": raw_code.map(lambda value: normalize_display_symbol("hk", value)),
            "provider_symbol": raw_code.map(lambda value: normalize_display_symbol("hk", value)),
            "name": _pick_series(frame, ["英文名称", "中文名称"]).astype(str),
            "cname": _pick_series(frame, ["中文名称", "英文名称"]).astype(str),
            "open": _pick_series(frame, ["今开"]),
            "close": _pick_series(frame, ["最新价"]),
            "high": _pick_series(frame, ["最高"]),
            "low": _pick_series(frame, ["最低"]),
            "volume": _pick_series(frame, ["成交量"]),
            "amount": _pick_series(frame, ["成交额"]),
            "trade_date": trade_date,
            "source": "akshare.stock_hk_spot",
        }
    )
    return _attach_api_time_attrs(normalized, frame, ["日期时间"])


def _normalize_us(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    raw_symbol = _pick_series(frame, ["symbol"]).astype(str)
    normalized = pd.DataFrame(
        {
            "market": "us",
            "board": "",
            "symbol": raw_symbol.map(lambda value: normalize_display_symbol("us", value)),
            "provider_symbol": raw_symbol.map(normalize_symbol),
            "name": _pick_series(frame, ["name", "cname"]).astype(str),
            "cname": _pick_series(frame, ["cname", "name"]).astype(str),
            "open": _pick_series(frame, ["open"]),
            "close": _pick_series(frame, ["price"]),
            "high": _pick_series(frame, ["high"]),
            "low": _pick_series(frame, ["low"]),
            "volume": _pick_series(frame, ["volume"]),
            "amount": pd.NA,
            "trade_date": trade_date,
            "source": "akshare.stock_us_spot",
        }
    )
    return _attach_api_time_attrs(normalized, frame, ["datetime", "date", "time", "ticktime"])


def _normalize_numeric(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    sample_api_time_field = normalized.attrs.get("sample_api_time_field")
    sample_api_time_value = normalized.attrs.get("sample_api_time_value")
    for column in ["open", "close", "high", "low", "volume", "amount"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized.drop_duplicates(subset=["board", "symbol"], keep="first", inplace=True)
    normalized.sort_values(by=["board", "symbol"], inplace=True, ignore_index=True)
    normalized = normalized[MARKET_DAY_SPOT_COLUMNS]
    normalized.attrs["sample_api_time_field"] = sample_api_time_field
    normalized.attrs["sample_api_time_value"] = sample_api_time_value
    return normalized


class AkshareMarketDaySpotProvider(MarketDaySpotProvider):
    """Fetch flattened market-level day spot data."""

    def fetch_market_day_spot(self, request: MarketDaySpotRequest) -> pd.DataFrame:
        market: MarketName = request.market
        try:
            if market == "cn":
                raw = ak.stock_zh_a_spot()
                return _normalize_numeric(_normalize_cn(raw, request.trade_date))
            if market == "hk":
                raw = ak.stock_hk_spot()
                return _normalize_numeric(_normalize_hk(raw, request.trade_date))
            if market == "us":
                raw = ak.stock_us_spot()
                return _normalize_numeric(_normalize_us(raw, request.trade_date))
        except Exception as exc:  # pragma: no cover - network/runtime
            raise DataProviderError(str(exc)) from exc
        raise DataProviderError(f"unsupported market: {market}")

"""Sina-based single-symbol history provider."""

from __future__ import annotations

import warnings

import akshare as ak
import pandas as pd
import requests
from akshare.stock.cons import hk_js_decode, zh_js_decode, zh_sina_a_stock_amount_url, zh_sina_a_stock_hist_url
from akshare.utils import demjson
from py_mini_racer import MiniRacer

from src.data.models import NUMERIC_COLUMNS, SYMBOL_HIST_COLUMNS, SymbolHistRequest, to_yyyymmdd
from src.data.providers.base import DataProviderError, SymbolHistProvider


def _empty_hist() -> pd.DataFrame:
    return pd.DataFrame(columns=SYMBOL_HIST_COLUMNS)


def _pick_series(frame: pd.DataFrame, aliases: list[str], default=pd.NA) -> pd.Series:
    for alias in aliases:
        if alias in frame.columns:
            return frame[alias]
    return pd.Series([default] * len(frame), index=frame.index)


def _normalize_datetime_index(index_like) -> pd.DatetimeIndex:
    series = pd.to_datetime(index_like, errors="coerce", utc=True)
    if isinstance(series, pd.Series):
        return pd.DatetimeIndex(series.dt.tz_localize(None))
    return series.tz_localize(None)


def _fetch_cn_history(provider_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    response = requests.get(zh_sina_a_stock_hist_url.format(provider_symbol), timeout=20)
    response.raise_for_status()

    js_code = MiniRacer()
    js_code.eval(hk_js_decode)
    dict_list = js_code.call("d", response.text.split("=")[1].split(";")[0].replace('"', ""))
    data_df = pd.DataFrame(dict_list)
    if data_df.empty:
        return pd.DataFrame()

    data_df.index = _normalize_datetime_index(data_df["date"])
    del data_df["date"]
    for column in ["prevclose", "postVol", "postAmt"]:
        if column in data_df.columns:
            del data_df[column]
    data_df = data_df.astype(float)

    amount_response = requests.get(
        zh_sina_a_stock_amount_url.format(provider_symbol, provider_symbol),
        timeout=20,
    )
    amount_response.raise_for_status()
    amount_json = demjson.decode(
        amount_response.text[amount_response.text.find("[") : amount_response.text.rfind("]") + 1]
    )
    amount_df = pd.DataFrame(amount_json)
    if not amount_df.empty:
        amount_df.columns = ["date", "outstanding_share"]
        amount_df.index = _normalize_datetime_index(amount_df["date"])
        del amount_df["date"]
        merged = pd.merge(data_df, amount_df, left_index=True, right_index=True, how="outer")
        merged.ffill(inplace=True)
    else:
        merged = data_df.copy()
        merged["outstanding_share"] = pd.NA

    if "outstanding_share" in merged.columns:
        merged["outstanding_share"] = pd.to_numeric(merged["outstanding_share"], errors="coerce") * 10000
        merged["turnover_rate"] = merged["volume"] / merged["outstanding_share"]
    else:
        merged["turnover_rate"] = pd.NA

    merged.columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "outstanding_share",
        "turnover_rate",
    ]
    mask = (merged.index >= pd.to_datetime(start_date)) & (merged.index <= pd.to_datetime(end_date))
    merged = merged.loc[mask].copy()
    if merged.empty:
        return pd.DataFrame()
    merged.index.name = "trade_date"
    merged.reset_index(inplace=True)
    return merged


def _normalize_cn_hist(raw: pd.DataFrame, request: SymbolHistRequest) -> pd.DataFrame:
    if raw is None or raw.empty:
        return _empty_hist()
    return pd.DataFrame(
        {
            "market": request.market,
            "board": request.board,
            "symbol": request.symbol,
            "provider_symbol": request.provider_symbol or request.symbol,
            "trade_date": raw["trade_date"],
            "open": raw["open"],
            "close": raw["close"],
            "high": raw["high"],
            "low": raw["low"],
            "volume": raw["volume"],
            "amount": raw["amount"],
            "amplitude": pd.NA,
            "change_pct": pd.NA,
            "change": pd.NA,
            "turnover_rate": raw["turnover_rate"] if "turnover_rate" in raw.columns else pd.NA,
            "source": "sina.cn_history",
        }
    )


def _normalize_hk_hist(raw: pd.DataFrame, request: SymbolHistRequest) -> pd.DataFrame:
    if raw is None or raw.empty:
        return _empty_hist()
    return pd.DataFrame(
        {
            "market": request.market,
            "board": request.board,
            "symbol": request.symbol,
            "provider_symbol": request.provider_symbol or request.symbol,
            "trade_date": _pick_series(raw, ["date"]),
            "open": _pick_series(raw, ["open"]),
            "close": _pick_series(raw, ["close"]),
            "high": _pick_series(raw, ["high"]),
            "low": _pick_series(raw, ["low"]),
            "volume": _pick_series(raw, ["volume"]),
            "amount": _pick_series(raw, ["amount"]),
            "amplitude": pd.NA,
            "change_pct": pd.NA,
            "change": pd.NA,
            "turnover_rate": pd.NA,
            "source": "akshare.stock_hk_daily",
        }
    )


def _normalize_us_hist(raw: pd.DataFrame, request: SymbolHistRequest) -> pd.DataFrame:
    if raw is None or raw.empty:
        return _empty_hist()
    return pd.DataFrame(
        {
            "market": request.market,
            "board": request.board,
            "symbol": request.symbol,
            "provider_symbol": request.provider_symbol or request.symbol,
            "trade_date": _pick_series(raw, ["date"]),
            "open": _pick_series(raw, ["open"]),
            "close": _pick_series(raw, ["close"]),
            "high": _pick_series(raw, ["high"]),
            "low": _pick_series(raw, ["low"]),
            "volume": _pick_series(raw, ["volume"]),
            "amount": _pick_series(raw, ["amount"]),
            "amplitude": pd.NA,
            "change_pct": pd.NA,
            "change": pd.NA,
            "turnover_rate": pd.NA,
            "source": "akshare.stock_us_daily",
        }
    )


def _filter_date_range(frame: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame
    normalized = frame.copy()
    trade_date = pd.to_datetime(_pick_series(normalized, ["date", "trade_date"]), errors="coerce")
    mask = (trade_date >= pd.to_datetime(start_date)) & (trade_date <= pd.to_datetime(end_date))
    return normalized.loc[mask].copy()


def _fetch_hk_daily_with_fallback(symbol: str, adjust: str) -> tuple[pd.DataFrame, str]:
    try:
        return ak.stock_hk_daily(symbol=symbol, adjust=adjust), adjust
    except Exception:
        if adjust == "":
            raise
        return ak.stock_hk_daily(symbol=symbol, adjust=""), ""


def _fetch_us_daily_with_fallback(symbol: str, adjust: str) -> tuple[pd.DataFrame, str]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            return ak.stock_us_daily(symbol=symbol, adjust=adjust), adjust
    except Exception:
        raw = _fetch_us_history_raw(symbol)
        return raw, "raw"


def _fetch_us_history_raw(symbol: str) -> pd.DataFrame:
    response = requests.get(f"https://finance.sina.com.cn/staticdata/us/{symbol}", timeout=20)
    response.raise_for_status()
    text = response.text.strip()
    if "=" not in text:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])

    js_code = MiniRacer()
    js_code.eval(zh_js_decode)
    try:
        dict_list = js_code.call("d", text.split("=")[1].split(";")[0].replace('"', ""))
    except Exception:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    if not dict_list:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])

    data_df = pd.DataFrame(dict_list)
    if "date" not in data_df.columns:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    if "amount" not in data_df.columns:
        data_df["amount"] = pd.NA
    return data_df[["date", "open", "high", "low", "close", "volume", "amount"]]


def _finalize(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return _empty_hist()
    normalized = frame.copy()
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized.dropna(subset=["trade_date"], inplace=True)
    for column in NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized.sort_values(by=["trade_date"], inplace=True, ignore_index=True)
    return normalized[SYMBOL_HIST_COLUMNS]


class AkshareSymbolHistProvider(SymbolHistProvider):
    """Fetch flattened single-symbol historical series using Sina endpoints only."""

    def fetch_symbol_hist(self, request: SymbolHistRequest) -> pd.DataFrame:
        start_date = to_yyyymmdd(request.start_date)
        end_date = to_yyyymmdd(request.end_date)
        try:
            if request.market == "cn":
                raw = _fetch_cn_history(request.provider_symbol or f"{request.board}{request.symbol}", start_date, end_date)
                return _finalize(_normalize_cn_hist(raw, request))
            if request.market == "hk":
                raw, used_adjust = _fetch_hk_daily_with_fallback(
                    symbol=request.provider_symbol or request.symbol,
                    adjust=request.adjust,
                )
                raw = _filter_date_range(raw, start_date, end_date)
                frame = _normalize_hk_hist(raw, request)
                frame["source"] = "akshare.stock_hk_daily" if used_adjust == request.adjust else "akshare.stock_hk_daily_raw_fallback"
                return _finalize(frame)
            if request.market == "us":
                raw, used_adjust = _fetch_us_daily_with_fallback(
                    symbol=request.provider_symbol or request.symbol,
                    adjust=request.adjust,
                )
                raw = _filter_date_range(raw, start_date, end_date)
                frame = _normalize_us_hist(raw, request)
                frame["source"] = "akshare.stock_us_daily" if used_adjust == request.adjust else "sina.us_history_raw_fallback"
                return _finalize(frame)
        except Exception as exc:  # pragma: no cover - network/runtime
            raise DataProviderError(str(exc)) from exc
        raise DataProviderError(f"unsupported market: {request.market}")

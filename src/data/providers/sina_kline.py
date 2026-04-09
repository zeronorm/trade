"""Sina history provider for 2-year single-symbol daily bars."""

from __future__ import annotations

import akshare as ak
import pandas as pd
import py_mini_racer
import requests
from akshare.stock.cons import (
    hk_js_decode,
    zh_js_decode,
    zh_sina_a_stock_amount_url,
    zh_sina_a_stock_hist_url,
)
from akshare.utils import demjson

from src.data.models import DAILY_COLUMNS, HistoryRequest, NUMERIC_COLUMNS, to_yyyymmdd
from src.data.providers.base import DailyKlineProvider, SnapshotProviderError


def _empty_daily() -> pd.DataFrame:
    return pd.DataFrame(columns=DAILY_COLUMNS)


def _finalize_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return _empty_daily()
    normalized = frame.copy()
    for column in DAILY_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    for column in NUMERIC_COLUMNS:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized.dropna(subset=["trade_date"], inplace=True)
    normalized.sort_values(by=["trade_date"], inplace=True, ignore_index=True)
    return normalized[DAILY_COLUMNS]


def _infer_cn_provider_symbol(symbol: str) -> str:
    if symbol.startswith(("sh", "sz", "bj")):
        return symbol
    if symbol.startswith(("4", "8", "92")):
        return f"bj{symbol}"
    if symbol.startswith(("5", "6", "9")):
        return f"sh{symbol}"
    return f"sz{symbol}"


def _normalize_a_symbol(symbol: str) -> str:
    text = str(symbol).strip().lower()
    if text.endswith(".0"):
        text = text[:-2]
    if text.startswith(("sh", "sz", "bj")):
        return text[2:]
    return text


def _normalize_hk_symbol(symbol: str) -> str:
    text = str(symbol).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(5)


def _normalize_datetime_index(index_like) -> pd.DatetimeIndex:
    series = pd.to_datetime(index_like, errors="coerce", utc=True)
    if isinstance(series, pd.Series):
        return pd.DatetimeIndex(series.dt.tz_localize(None))
    return series.tz_localize(None)


def _fetch_a_history(provider_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    response = requests.get(zh_sina_a_stock_hist_url.format(provider_symbol), timeout=15)
    response.raise_for_status()
    js_code = py_mini_racer.MiniRacer()
    js_code.eval(hk_js_decode)
    dict_list = js_code.call(
        "d",
        response.text.split("=")[1].split(";")[0].replace('"', ""),
    )
    data_df = pd.DataFrame(dict_list)
    data_df.index = _normalize_datetime_index(data_df["date"])
    del data_df["date"]
    for column in ["prevclose", "postVol", "postAmt"]:
        if column in data_df.columns:
            del data_df[column]
    data_df = data_df.astype(float)

    amount_response = requests.get(zh_sina_a_stock_amount_url.format(provider_symbol, provider_symbol), timeout=15)
    amount_response.raise_for_status()
    amount_json = demjson.decode(
        amount_response.text[amount_response.text.find("[") : amount_response.text.rfind("]") + 1]
    )
    amount_df = pd.DataFrame(amount_json)
    amount_df.columns = ["date", "outstanding_share"]
    amount_df.index = _normalize_datetime_index(amount_df["date"])
    del amount_df["date"]

    merged = pd.merge(data_df, amount_df, left_index=True, right_index=True, how="outer")
    merged.ffill(inplace=True)
    merged = merged.astype(float)
    merged["outstanding_share"] = merged["outstanding_share"] * 10000
    merged["turnover"] = merged["volume"] / merged["outstanding_share"]
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
    merged.index.name = "trade_date"
    merged.reset_index(inplace=True)
    merged.rename(columns={"date": "trade_date", "index": "trade_date"}, inplace=True)
    return merged


def _fetch_us_history_raw(provider_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    response = requests.get(f"https://finance.sina.com.cn/staticdata/us/{provider_symbol}", timeout=15)
    response.raise_for_status()
    text = response.text.strip()
    if "=" not in text:
        return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume", "amount"])

    js_code = py_mini_racer.MiniRacer()
    js_code.eval(zh_js_decode)
    try:
        dict_list = js_code.call("d", text.split("=")[1].split(";")[0].replace('"', ""))
    except Exception:
        return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume", "amount"])
    if not dict_list:
        return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume", "amount"])

    data_df = pd.DataFrame(dict_list)
    if "date" not in data_df.columns:
        return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume", "amount"])

    data_df["trade_date"] = pd.to_datetime(data_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    data_df = data_df[
        (pd.to_datetime(data_df["trade_date"]) >= pd.to_datetime(start_date))
        & (pd.to_datetime(data_df["trade_date"]) <= pd.to_datetime(end_date))
    ].copy()
    if data_df.empty:
        return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume", "amount"])

    for column in ["open", "high", "low", "close", "volume"]:
        if column in data_df.columns:
            data_df[column] = pd.to_numeric(data_df[column], errors="coerce")
    if "amount" in data_df.columns:
        data_df["amount"] = pd.to_numeric(data_df["amount"], errors="coerce")
    else:
        data_df["amount"] = pd.NA
    return data_df[["trade_date", "open", "high", "low", "close", "volume", "amount"]]


class SinaHistoryKlineProvider(DailyKlineProvider):
    """Fetch 2-year history per ticker."""

    def fetch_history(self, request: HistoryRequest) -> pd.DataFrame:
        market = request.market
        provider_symbol = request.provider_symbol
        if market == "a":
            provider_symbol = _infer_cn_provider_symbol(_normalize_a_symbol(provider_symbol or request.symbol))
        elif market == "hk":
            provider_symbol = _normalize_hk_symbol(provider_symbol or request.symbol)
        else:
            provider_symbol = provider_symbol or request.symbol

        start_date = to_yyyymmdd(request.start_date)
        end_date = to_yyyymmdd(request.end_date)

        try:
            if market == "a":
                raw = _fetch_a_history(provider_symbol, start_date, end_date)
            elif market == "hk":
                raw = ak.stock_hk_daily(symbol=provider_symbol, adjust=request.adjust)
                if raw is None or raw.empty or "date" not in raw.columns:
                    raise SnapshotProviderError(f"empty hk daily response for symbol={provider_symbol}")
                raw["trade_date"] = pd.to_datetime(raw["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                raw = raw[
                    (pd.to_datetime(raw["trade_date"]) >= pd.to_datetime(start_date))
                    & (pd.to_datetime(raw["trade_date"]) <= pd.to_datetime(end_date))
                ]
            elif market == "us":
                if request.adjust == "":
                    raw = _fetch_us_history_raw(provider_symbol, start_date, end_date)
                else:
                    raw = ak.stock_us_daily(symbol=provider_symbol, adjust=request.adjust)
                    if raw is None or raw.empty or "date" not in raw.columns:
                        raise SnapshotProviderError(f"empty us daily response for symbol={provider_symbol}")
                    raw["trade_date"] = pd.to_datetime(raw["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                    raw = raw[
                        (pd.to_datetime(raw["trade_date"]) >= pd.to_datetime(start_date))
                        & (pd.to_datetime(raw["trade_date"]) <= pd.to_datetime(end_date))
                    ]
            else:
                raise SnapshotProviderError(f"unsupported market: {market}")
        except Exception as exc:  # pragma: no cover - network/runtime
            raise SnapshotProviderError(str(exc)) from exc

        frame = pd.DataFrame(
            {
                "market": market,
                "symbol": request.symbol,
                "provider_symbol": provider_symbol,
                "trade_date": raw["trade_date"],
                "open": raw["open"],
                "high": raw["high"],
                "low": raw["low"],
                "close": raw["close"],
                "volume": raw["volume"],
                "amount": raw["amount"] if "amount" in raw.columns else pd.NA,
                "turnover_rate": raw["turnover_rate"] if "turnover_rate" in raw.columns else raw["turnover"] if "turnover" in raw.columns else pd.NA,
                "outstanding_share": raw["outstanding_share"] if "outstanding_share" in raw.columns else pd.NA,
                "source": "sina",
            }
        )
        return _finalize_daily(frame)

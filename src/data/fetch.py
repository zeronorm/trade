"""Fetch market data from akshare / sina."""

from __future__ import annotations

import logging
import warnings

import akshare as ak
import execjs
import pandas as pd
import requests
from akshare.stock.cons import (
    hk_js_decode,
    zh_js_decode,
    zh_sina_a_stock_amount_url,
    zh_sina_a_stock_hist_url,
)
from akshare.utils import demjson

from src.data.models import (
    HIST_COLUMNS,
    SPOT_COLUMNS,
    clean,
    detect_board,
    display_symbol,
    history_start_date,
    provider_symbol,
    to_yyyymmdd,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Day spot (full market snapshot)
# ---------------------------------------------------------------------------

def _pick(frame: pd.DataFrame, aliases: list[str], default=pd.NA) -> pd.Series:
    for a in aliases:
        if a in frame.columns:
            return frame[a]
    return pd.Series([default] * len(frame), index=frame.index)


def _numeric(frame: pd.DataFrame) -> pd.DataFrame:
    for col in ["open", "close", "high", "low", "volume", "amount"]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame.drop_duplicates(subset=["board", "symbol"], keep="first", inplace=True)
    frame.sort_values(by=["board", "symbol"], inplace=True, ignore_index=True)
    return frame[SPOT_COLUMNS]


def fetch_spot_cn(trade_date: str) -> pd.DataFrame:
    raw = ak.stock_zh_a_spot()
    code = raw["代码"].astype(str)
    board = code.map(detect_board)
    sym = code.map(lambda v: display_symbol("cn", v))
    df = pd.DataFrame({
        "market": "cn", "board": board, "symbol": sym,
        "provider_symbol": board.str.cat(sym),
        "name": _pick(raw, ["名称"]).astype(str),
        "cname": _pick(raw, ["名称"]).astype(str),
        "open": raw["今开"], "close": raw["最新价"],
        "high": raw["最高"], "low": raw["最低"],
        "volume": raw["成交量"], "amount": raw["成交额"],
        "trade_date": trade_date, "source": "sina.stock_zh_a_spot",
    })
    return _numeric(df)


def fetch_spot_hk(trade_date: str) -> pd.DataFrame:
    raw = ak.stock_hk_spot()
    code = raw["代码"].astype(str)
    sym = code.map(lambda v: display_symbol("hk", v))
    df = pd.DataFrame({
        "market": "hk", "board": "", "symbol": sym,
        "provider_symbol": sym,
        "name": _pick(raw, ["英文名称", "中文名称"]).astype(str),
        "cname": _pick(raw, ["中文名称", "英文名称"]).astype(str),
        "open": raw["今开"], "close": raw["最新价"],
        "high": raw["最高"], "low": raw["最低"],
        "volume": raw["成交量"], "amount": raw["成交额"],
        "trade_date": trade_date, "source": "sina.stock_hk_spot",
    })
    return _numeric(df)


def fetch_spot_us(trade_date: str) -> pd.DataFrame:
    raw = ak.stock_us_spot()
    sym = _pick(raw, ["symbol"]).astype(str).map(lambda v: display_symbol("us", v))
    df = pd.DataFrame({
        "market": "us", "board": "", "symbol": sym,
        "provider_symbol": _pick(raw, ["symbol"]).astype(str),
        "name": _pick(raw, ["name"]).astype(str),
        "cname": _pick(raw, ["cname"]).astype(str),
        "open": _pick(raw, ["open"]), "close": _pick(raw, ["price"]),
        "high": _pick(raw, ["high"]), "low": _pick(raw, ["low"]),
        "volume": _pick(raw, ["volume"]), "amount": pd.NA,
        "trade_date": trade_date, "source": "sina.stock_us_spot",
    })
    return _numeric(df)


SPOT_FETCHERS = {"cn": fetch_spot_cn, "hk": fetch_spot_hk, "us": fetch_spot_us}

def fetch_spot(market: str, trade_date: str) -> pd.DataFrame:
    return SPOT_FETCHERS[market](trade_date)

# ---------------------------------------------------------------------------
# Symbol history (single stock)
# ---------------------------------------------------------------------------

def _fetch_cn_hist(sym: str, start: str, end: str) -> pd.DataFrame:
    r = requests.get(zh_sina_a_stock_hist_url.format(sym), timeout=20)
    r.raise_for_status()
    ctx = execjs.compile(hk_js_decode)
    data = ctx.call("d", r.text.split("=")[1].split(";")[0].replace('"', ""))
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df.index = pd.DatetimeIndex(pd.to_datetime(df["date"], errors="coerce")).tz_localize(None)
    del df["date"]
    for col in ["prevclose", "postVol", "postAmt"]:
        if col in df.columns:
            del df[col]
    df = df.astype(float)

    ar = requests.get(zh_sina_a_stock_amount_url.format(sym, sym), timeout=20)
    ar.raise_for_status()
    aj = demjson.decode(ar.text[ar.text.find("["):ar.text.rfind("]") + 1])
    adf = pd.DataFrame(aj)
    if not adf.empty:
        adf.columns = ["date", "outstanding_share"]
        adf.index = pd.DatetimeIndex(pd.to_datetime(adf["date"], errors="coerce")).tz_localize(None)
        del adf["date"]
        merged = pd.merge(df, adf, left_index=True, right_index=True, how="outer")
        merged.ffill(inplace=True)
    else:
        merged = df.copy()
        merged["outstanding_share"] = pd.NA

    if "outstanding_share" in merged.columns:
        merged["outstanding_share"] = pd.to_numeric(merged["outstanding_share"], errors="coerce") * 10000
        merged["turnover_rate"] = merged["volume"] / merged["outstanding_share"]
    else:
        merged["turnover_rate"] = pd.NA

    merged.columns = ["open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover_rate"]
    mask = (merged.index >= pd.to_datetime(start)) & (merged.index <= pd.to_datetime(end))
    merged = merged.loc[mask].copy()
    if merged.empty:
        return pd.DataFrame()
    merged.index.name = "trade_date"
    merged.reset_index(inplace=True)
    return merged


def _fetch_us_hist_raw(sym: str) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    try:
        r = requests.get(f"https://finance.sina.com.cn/staticdata/us/{sym}", timeout=20)
        r.raise_for_status()
        text = r.text.strip()
        if "=" not in text:
            return empty
        ctx = execjs.compile(zh_js_decode)
        data = ctx.call("d", text.split("=")[1].split(";")[0].replace('"', ""))
        if not data:
            return empty
        df = pd.DataFrame(data)
        if "date" not in df.columns:
            return empty
        if "amount" not in df.columns:
            df["amount"] = pd.NA
        return df[["date", "open", "high", "low", "close", "volume", "amount"]]
    except Exception:
        return empty


def fetch_hist(market: str, board: str, sym: str, *,
               trade_date: str, provider_sym: str = "", adjust: str = "qfq") -> pd.DataFrame:
    start = history_start_date(trade_date)
    end = to_yyyymmdd(trade_date)

    if market == "cn":
        ps = provider_sym or f"{board}{sym}"
        raw = _fetch_cn_hist(ps, start, end)
        src = "sina.cn_history"
    elif market == "hk":
        ps = provider_sym or sym
        try:
            raw = ak.stock_hk_daily(symbol=ps, adjust=adjust)
        except Exception:
            raw = ak.stock_hk_daily(symbol=ps, adjust="")
        src = "akshare.stock_hk_daily"
    elif market == "us":
        ps = provider_sym or sym
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", FutureWarning)
                raw = ak.stock_us_daily(symbol=ps, adjust=adjust)
            src = "akshare.stock_us_daily"
        except Exception:
            raw = _fetch_us_hist_raw(ps)
            src = "sina.us_history_raw"
    else:
        raise ValueError(f"unsupported market: {market}")

    if raw is None or raw.empty:
        return pd.DataFrame(columns=HIST_COLUMNS)

    # filter date
    date_col = "trade_date" if "trade_date" in raw.columns else "date"
    dates = pd.to_datetime(raw[date_col], errors="coerce")
    mask = (dates >= pd.to_datetime(start)) & (dates <= pd.to_datetime(end))
    raw = raw.loc[mask].copy()
    if raw.empty:
        return pd.DataFrame(columns=HIST_COLUMNS)

    return pd.DataFrame({
        "market": market, "board": clean(board), "symbol": sym,
        "provider_symbol": ps,
        "trade_date": raw[date_col],
        "open": _pick(raw, ["open"]), "close": _pick(raw, ["close"]),
        "high": _pick(raw, ["high"]), "low": _pick(raw, ["low"]),
        "volume": _pick(raw, ["volume"]), "amount": _pick(raw, ["amount"]),
        "amplitude": pd.NA, "change_pct": pd.NA, "change": pd.NA,
        "turnover_rate": raw["turnover_rate"] if "turnover_rate" in raw.columns else pd.NA,
        "source": src,
    })

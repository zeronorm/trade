"""Sina snapshot provider built on top of Akshare Sina spot interfaces."""

from __future__ import annotations

import json
import time
from typing import Iterable

import akshare as ak
import pandas as pd
import requests
from akshare.utils import demjson

from src.data.models import NUMERIC_COLUMNS, SNAPSHOT_COLUMNS, SnapshotRequest
from src.data.providers.base import SnapshotProvider, SnapshotProviderError


def _pick_series(frame: pd.DataFrame, aliases: Iterable[str], default=pd.NA) -> pd.Series:
    for alias in aliases:
        if alias in frame.columns:
            return frame[alias]
    if frame.empty:
        return pd.Series(dtype="object")
    return pd.Series([default] * len(frame), index=frame.index)


def _infer_cn_provider_symbol(symbol: str) -> str:
    if symbol.startswith(("sh", "sz", "bj")):
        return symbol
    if symbol.startswith(("4", "8", "92")):
        return f"bj{symbol}"
    if symbol.startswith(("5", "6", "9")):
        return f"sh{symbol}"
    return f"sz{symbol}"


def _normalize_a_symbol(value) -> str:
    text = str(value).strip().lower()
    if text.endswith(".0"):
        text = text[:-2]
    if text.startswith(("sh", "sz", "bj")):
        return text[2:]
    return text


def _normalize_market_symbol(value, market: str) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if market == "a":
        return _normalize_a_symbol(text)
    if market == "hk":
        return text.zfill(5)
    return text


def _normalize_snapshot(frame: pd.DataFrame, market: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    raw_symbol = _pick_series(frame, ["代码", "symbol", "代码\u3000", "code"]).astype(str)
    symbol = raw_symbol.map(lambda value: _normalize_market_symbol(value, market))
    if market == "a":
        provider_symbol = raw_symbol.map(lambda value: _infer_cn_provider_symbol(_normalize_a_symbol(value)))
    else:
        provider_symbol = symbol.astype(str)

    normalized = pd.DataFrame(
        {
            "market": market,
            "symbol": symbol,
            "provider_symbol": provider_symbol,
            "name": _pick_series(frame, ["名称", "name", "中文名称", "cname", "英文名称", "enname"]).astype(str),
            "open": _pick_series(frame, ["今开", "open"]),
            "high": _pick_series(frame, ["最高", "high"]),
            "low": _pick_series(frame, ["最低", "low"]),
            "close": _pick_series(frame, ["最新价", "trade", "last", "lasttrade", "price"]),
            "prev_close": _pick_series(frame, ["昨收", "prev_close", "settlement", "prevclose"]),
            "volume": _pick_series(frame, ["成交量", "volume"]),
            "amount": _pick_series(frame, ["成交额", "amount"]),
            "turnover_rate": _pick_series(frame, ["换手率", "turnover_rate", "turnoverratio", "changepercent"]),
            "source": "sina",
        }
    )
    for column in NUMERIC_COLUMNS:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(
                normalized[column].astype(str).str.rstrip("%"),
                errors="coerce",
            )
    normalized.drop_duplicates(subset=["symbol"], keep="first", inplace=True)
    normalized.sort_values(by=["symbol"], inplace=True, ignore_index=True)
    return normalized[SNAPSHOT_COLUMNS]


def _call_with_retry(fetcher, *, attempts: int = 3, pause_seconds: float = 1.0) -> pd.DataFrame:
    last_exception: Exception | None = None
    for attempt in range(1, max(attempts, 1) + 1):
        try:
            return fetcher()
        except Exception as exc:  # pragma: no cover - network/runtime
            last_exception = exc
            if attempt < max(attempts, 1):
                time.sleep(pause_seconds)
    raise SnapshotProviderError(str(last_exception))


def _parse_hk_payload(text: str):
    stripped = text.strip()
    if not stripped:
        return []
    try:
        return json.loads(stripped)
    except Exception:
        return demjson.decode(stripped)


def _fetch_hk_snapshot_via_requests() -> pd.DataFrame:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            "Referer": "https://vip.stock.finance.sina.com.cn/mkt/#qbgg_hk",
            "Accept": "application/json,text/plain,*/*",
        }
    )
    preheat_urls = [
        "https://vip.stock.finance.sina.com.cn/mkt/#qbgg_hk",
        "http://vip.stock.finance.sina.com.cn/mkt/#qbgg_hk",
    ]
    endpoints = [
        "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHKStockData",
        "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHKStockData",
    ]
    params = {
        "page": "1",
        "num": "3000",
        "sort": "symbol",
        "asc": "1",
        "node": "qbgg_hk",
        "_s_r_a": "page",
    }
    last_exception: Exception | None = None
    for endpoint in endpoints:
        frames: list[pd.DataFrame] = []
        try:
            for preheat_url in preheat_urls:
                try:
                    session.get(preheat_url, timeout=20)
                    break
                except Exception:
                    continue
            for page in range(1, 20):
                params["page"] = str(page)
                response = session.get(endpoint, params=params, timeout=20)
                response.raise_for_status()
                payload = _parse_hk_payload(response.text)
                if not payload:
                    break
                frames.append(pd.DataFrame(payload))
            if frames:
                return pd.concat(frames, ignore_index=True)
        except Exception as exc:  # pragma: no cover - network/runtime
            last_exception = exc
            continue
    raise SnapshotProviderError(str(last_exception or "hk snapshot request returned no data"))


class SinaSnapshotProvider(SnapshotProvider):
    """Fetch snapshots for A/HK/US markets."""

    def fetch_snapshot(self, request: SnapshotRequest) -> pd.DataFrame:
        market = request.market
        try:
            if market == "a":
                merged = _call_with_retry(ak.stock_zh_a_spot)
            elif market == "hk":
                try:
                    merged = _call_with_retry(ak.stock_hk_spot)
                except SnapshotProviderError:
                    merged = _fetch_hk_snapshot_via_requests()
            elif market == "us":
                merged = _call_with_retry(ak.stock_us_spot)
            else:
                raise SnapshotProviderError(f"unsupported market: {market}")
        except Exception as exc:  # pragma: no cover - network/runtime
            raise SnapshotProviderError(str(exc)) from exc
        return _normalize_snapshot(merged, market)

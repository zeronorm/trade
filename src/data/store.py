"""CSV read/write helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DAY_ROOT = Path("data_store/day")
HIST_ROOT = Path("data_store/hist")

_DTYPE = {
    "market": "string", "board": "string", "symbol": "string",
    "provider_symbol": "string", "trade_date": "string",
}


def _read(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, keep_default_na=False, low_memory=False, dtype=_DTYPE)


def _write(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


# --- day spot ---

def day_path(market: str, trade_date: str) -> Path:
    return DAY_ROOT / f"{market}.{trade_date}.csv"


def day_exists(market: str, trade_date: str) -> bool:
    return day_path(market, trade_date).exists()


def save_day(frame: pd.DataFrame, market: str, trade_date: str) -> Path:
    return _write(frame, day_path(market, trade_date))


def load_day(market: str, trade_date: str) -> pd.DataFrame:
    return _read(day_path(market, trade_date))


def load_prev_day(market: str, trade_date: str) -> pd.DataFrame | None:
    cur = day_path(market, trade_date)
    older = sorted(
        p for p in DAY_ROOT.glob(f"{market}.*.csv")
        if p != cur and p.stem.split(".", 1)[1] < trade_date
    )
    return _read(older[-1]) if older else None


# --- history ---

def hist_path(market: str, trade_date: str) -> Path:
    return HIST_ROOT / f"{market}.{trade_date}.csv"


def hist_exists(market: str, trade_date: str) -> bool:
    return hist_path(market, trade_date).exists()


def save_hist(frame: pd.DataFrame, market: str, trade_date: str) -> Path:
    return _write(frame, hist_path(market, trade_date))


def load_hist(market: str, trade_date: str) -> pd.DataFrame:
    return _read(hist_path(market, trade_date))

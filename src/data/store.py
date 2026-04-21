"""CSV storage helpers for day spot, daily hist, and merged hist."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.models import HIST_COLUMNS

DAY_ROOT = Path("data_store/day")
HIST_ROOT = Path("data_store/hist")
HIST_PROGRESS_COLUMNS = ["board", "symbol", "provider_symbol", "has_data"]

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


def _append(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, mode="a", header=not path.exists())
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


# --- history ---

def hist_path(market: str, trade_date: str) -> Path:
    return HIST_ROOT / f"{market}.{trade_date}.csv"


def hist_merge_path(market: str, trade_date: str) -> Path:
    return HIST_ROOT / f"{market}.merge.{trade_date}.csv"


def hist_progress_path(market: str, trade_date: str) -> Path:
    return HIST_ROOT / f"{market}.{trade_date}.progress.csv"


def hist_exists(market: str, trade_date: str) -> bool:
    return hist_path(market, trade_date).exists()


def hist_merge_exists(market: str, trade_date: str) -> bool:
    return hist_merge_path(market, trade_date).exists()


def hist_progress_exists(market: str, trade_date: str) -> bool:
    return hist_progress_path(market, trade_date).exists()


def save_hist(frame: pd.DataFrame, market: str, trade_date: str) -> Path:
    return _write(frame, hist_path(market, trade_date))


def load_hist(market: str, trade_date: str) -> pd.DataFrame:
    return _read(hist_path(market, trade_date))


def append_hist(frame: pd.DataFrame, market: str, trade_date: str) -> Path:
    return _append(frame.reindex(columns=HIST_COLUMNS), hist_path(market, trade_date))


def compact_hist(market: str, trade_date: str) -> pd.DataFrame:
    if not hist_exists(market, trade_date):
        return pd.DataFrame(columns=HIST_COLUMNS)

    frame = load_hist(market, trade_date).reindex(columns=HIST_COLUMNS)
    if frame.empty:
        save_hist(frame, market, trade_date)
        return frame

    frame.drop_duplicates(
        subset=["market", "board", "symbol", "trade_date"],
        keep="last",
        inplace=True,
        ignore_index=True,
    )
    frame.sort_values(by=["board", "symbol", "trade_date"], inplace=True, ignore_index=True)
    save_hist(frame, market, trade_date)
    return frame


def load_hist_progress(market: str, trade_date: str) -> pd.DataFrame:
    path = hist_progress_path(market, trade_date)
    if not path.exists():
        return pd.DataFrame(columns=HIST_PROGRESS_COLUMNS)

    frame = pd.read_csv(
        path,
        keep_default_na=False,
        low_memory=False,
        dtype={"board": "string", "symbol": "string", "provider_symbol": "string"},
    )
    frame = frame.reindex(columns=HIST_PROGRESS_COLUMNS)
    if not frame.empty:
        frame.drop_duplicates(subset=["board", "symbol"], keep="last", inplace=True, ignore_index=True)
        frame.sort_values(by=["board", "symbol"], inplace=True, ignore_index=True)
    return frame


def append_hist_progress(
    market: str,
    trade_date: str,
    *,
    board: str,
    symbol: str,
    provider_symbol: str,
    has_data: bool,
) -> Path:
    frame = pd.DataFrame(
        [{
            "board": board,
            "symbol": symbol,
            "provider_symbol": provider_symbol,
            "has_data": has_data,
        }],
        columns=HIST_PROGRESS_COLUMNS,
    )
    return _append(frame, hist_progress_path(market, trade_date))


def list_hist_paths(market: str, *, upto_trade_date: str | None = None) -> list[Path]:
    paths: list[Path] = []
    for path in HIST_ROOT.glob(f"{market}.*.csv"):
        parts = path.stem.split(".")
        if len(parts) != 2:
            continue
        file_trade_date = parts[1]
        if upto_trade_date is not None and file_trade_date > upto_trade_date:
            continue
        paths.append(path)
    return sorted(paths, key=lambda path: path.stem.split(".")[1])


def load_prev_hist_merge(market: str, trade_date: str) -> pd.DataFrame | None:
    current = hist_merge_path(market, trade_date)
    older: list[Path] = []
    for path in HIST_ROOT.glob(f"{market}.merge.*.csv"):
        if path == current:
            continue
        parts = path.stem.split(".")
        if len(parts) != 3 or parts[1] != "merge" or parts[2] >= trade_date:
            continue
        older.append(path)
    older.sort()
    return _read(older[-1]) if older else None


def merge_hist(market: str, trade_date: str, current_frame: pd.DataFrame | None = None) -> Path:
    frames: list[pd.DataFrame] = []

    prev_merge = load_prev_hist_merge(market, trade_date)
    if prev_merge is not None and not prev_merge.empty:
        frames.append(prev_merge.reindex(columns=HIST_COLUMNS))
    else:
        for path in list_hist_paths(market, upto_trade_date=trade_date):
            if path.stem.split(".")[1] >= trade_date:
                continue
            frame = _read(path)
            if frame.empty:
                continue
            frames.append(frame.reindex(columns=HIST_COLUMNS))

    if current_frame is not None:
        if not current_frame.empty:
            frames.append(current_frame.reindex(columns=HIST_COLUMNS))
    elif hist_exists(market, trade_date):
        frame = load_hist(market, trade_date)
        if not frame.empty:
            frames.append(frame.reindex(columns=HIST_COLUMNS))

    if frames:
        merged = pd.concat(frames, ignore_index=True)
        merged.drop_duplicates(
            subset=["market", "board", "symbol", "trade_date"],
            keep="last",
            inplace=True,
            ignore_index=True,
        )
    else:
        merged = pd.DataFrame(columns=HIST_COLUMNS)

    if not merged.empty:
        merged.sort_values(by=["board", "symbol", "trade_date"], inplace=True, ignore_index=True)

    return _write(merged, hist_merge_path(market, trade_date))


def load_hist_merge(market: str, trade_date: str) -> pd.DataFrame:
    return _read(hist_merge_path(market, trade_date))

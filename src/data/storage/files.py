"""Filesystem storage for snapshots, latest daily files, and history files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


class SnapshotFileStore:
    def __init__(self, root: str | Path = "data_store/snapshots") -> None:
        self.root = Path(root)

    def build_path(self, market: str, trade_date: str) -> Path:
        return self.root / f"{market}.{trade_date}.csv"

    def save(self, frame: pd.DataFrame, market: str, trade_date: str) -> Path:
        path = self.build_path(market, trade_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False)
        return path

    def load(self, market: str, trade_date: str | None = None) -> tuple[pd.DataFrame, Path]:
        if trade_date:
            path = self.build_path(market, trade_date)
            return pd.read_csv(path), path
        candidates = sorted(self.root.glob(f"{market}.*.csv"))
        if not candidates:
            raise FileNotFoundError(f"no snapshot file for market={market}")
        path = candidates[-1]
        return pd.read_csv(path), path


class LatestDailyStore:
    def __init__(self, root: str | Path = "data_store/daily_latest") -> None:
        self.root = Path(root)

    def build_path(self, market: str, trade_date: str) -> Path:
        return self.root / f"{market}.{trade_date}.csv"

    def save(self, frame: pd.DataFrame, market: str, trade_date: str) -> Path:
        path = self.build_path(market, trade_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False)
        return path


class HistoryFileStore:
    def __init__(self, root: str | Path = "data_store/history_2y") -> None:
        self.root = Path(root)

    def build_path(self, market: str, symbol: str, end_date: str) -> Path:
        return self.root / market / f"{symbol}.{end_date}.history_2y.csv"

    def exists(self, market: str, symbol: str, end_date: str) -> bool:
        return self.build_path(market, symbol, end_date).exists()

    def save(self, frame: pd.DataFrame, market: str, symbol: str, end_date: str) -> Path:
        path = self.build_path(market, symbol, end_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False)
        return path

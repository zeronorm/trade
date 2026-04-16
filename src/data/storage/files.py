"""Filesystem storage for market day spot and symbol history files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.models import normalize_optional_text


def _read_csv(path: Path) -> pd.DataFrame:
    dtype = {
        "market": "string",
        "board": "string",
        "symbol": "string",
        "provider_symbol": "string",
        "trade_date": "string",
    }
    return pd.read_csv(path, keep_default_na=False, low_memory=False, dtype=dtype)


class MarketDaySpotStore:
    def __init__(self, root: str | Path = "data_store/day") -> None:
        self.root = Path(root)

    def build_path(self, market: str, trade_date: str) -> Path:
        return self.root / f"{market}.{trade_date}.csv"

    def save(self, frame: pd.DataFrame, market: str, trade_date: str) -> Path:
        path = self.build_path(market, trade_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False)
        return path

    def load(self, market: str, trade_date: str) -> tuple[pd.DataFrame, Path]:
        path = self.build_path(market, trade_date)
        return _read_csv(path), path

    def load_previous(self, market: str, trade_date: str) -> tuple[pd.DataFrame, Path] | None:
        current_path = self.build_path(market, trade_date)
        candidates = sorted(path for path in self.root.glob(f"{market}.*.csv") if path != current_path)
        older = [path for path in candidates if path.stem.split(".", 1)[1] < trade_date]
        if not older:
            return None
        path = older[-1]
        return _read_csv(path), path


class SymbolHistStore:
    def __init__(self, root: str | Path = "data_store/hist") -> None:
        self.root = Path(root)

    def build_path(self, market: str) -> Path:
        return self.root / f"{market}.csv"

    def load(self, market: str) -> tuple[pd.DataFrame, Path]:
        path = self.build_path(market)
        return _read_csv(path), path

    def load_or_empty(self, market: str) -> tuple[pd.DataFrame, Path]:
        path = self.build_path(market)
        if path.exists():
            return _read_csv(path), path
        return pd.DataFrame(), path

    def write(self, frame: pd.DataFrame, market: str) -> Path:
        path = self.build_path(market)
        path.parent.mkdir(parents=True, exist_ok=True)
        normalized = frame.copy()
        normalized.drop_duplicates(subset=["market", "board", "symbol", "trade_date"], keep="last", inplace=True)
        normalized.sort_values(by=["board", "symbol", "trade_date"], inplace=True, ignore_index=True)
        normalized.to_csv(path, index=False)
        return path

    def save(self, frame: pd.DataFrame, market: str) -> Path:
        path = self.build_path(market)
        existing, _ = self.load_or_empty(market)
        if existing.empty:
            merged = frame.copy()
        else:
            merged = pd.concat([existing, frame], ignore_index=True)
        return self.write(merged, market)

    def has_symbol_trade_date(self, market: str, board: str, symbol: str, trade_date: str) -> bool:
        existing, _ = self.load_or_empty(market)
        if existing.empty:
            return False
        normalized_board = normalize_optional_text(board)
        normalized_symbol = normalize_optional_text(symbol)
        normalized_trade_date = normalize_optional_text(trade_date)
        matches = (
            (existing["board"].map(normalize_optional_text) == normalized_board)
            & (existing["symbol"].map(normalize_optional_text) == normalized_symbol)
            & (existing["trade_date"].map(normalize_optional_text) == normalized_trade_date)
        )
        return bool(matches.any())

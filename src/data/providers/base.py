"""Provider abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from src.data.models import HistoryRequest, SnapshotRequest


class SnapshotProviderError(RuntimeError):
    """Raised when provider fetch or normalization fails."""


class SnapshotProvider(ABC):
    @abstractmethod
    def fetch_snapshot(self, request: SnapshotRequest) -> pd.DataFrame:
        raise NotImplementedError


class DailyKlineProvider(ABC):
    @abstractmethod
    def fetch_history(self, request: HistoryRequest) -> pd.DataFrame:
        raise NotImplementedError

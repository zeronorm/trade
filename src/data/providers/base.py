"""Provider abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from src.data.models import MarketDaySpotRequest, SymbolHistRequest


class DataProviderError(RuntimeError):
    """Raised when provider fetch or normalization fails."""


class MarketDaySpotProvider(ABC):
    @abstractmethod
    def fetch_market_day_spot(self, request: MarketDaySpotRequest) -> pd.DataFrame:
        raise NotImplementedError


class SymbolHistProvider(ABC):
    @abstractmethod
    def fetch_symbol_hist(self, request: SymbolHistRequest) -> pd.DataFrame:
        raise NotImplementedError

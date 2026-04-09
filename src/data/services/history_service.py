"""Single-symbol 2-year history service."""

from __future__ import annotations

from src.data.models import HistoryRequest, history_start_date, normalize_trade_date
from src.data.providers import SinaHistoryKlineProvider
from src.data.storage import HistoryFileStore


class HistoryKlineService:
    def __init__(
        self,
        provider: SinaHistoryKlineProvider | None = None,
        store: HistoryFileStore | None = None,
    ) -> None:
        self.provider = provider or SinaHistoryKlineProvider()
        self.store = store or HistoryFileStore()

    def fetch_symbol_history_2y(
        self,
        market: str,
        symbol: str,
        *,
        provider_symbol: str | None = None,
        end_date: str,
        adjust: str = "",
    ):
        normalized_end = normalize_trade_date(end_date)
        request = HistoryRequest(
            market=market,  # type: ignore[arg-type]
            symbol=symbol,
            provider_symbol=provider_symbol,
            end_date=normalized_end,
            start_date=history_start_date(normalized_end),
            adjust=adjust,
        )
        return self.provider.fetch_history(request)

    def fetch_and_store_symbol_history_2y(
        self,
        market: str,
        symbol: str,
        *,
        provider_symbol: str | None = None,
        end_date: str,
        adjust: str = "",
        skip_existing: bool = True,
    ):
        normalized_end = normalize_trade_date(end_date)
        if skip_existing and self.store.exists(market, symbol, normalized_end):
            return None, str(self.store.build_path(market, symbol, normalized_end))
        frame = self.fetch_symbol_history_2y(
            market,
            symbol,
            provider_symbol=provider_symbol,
            end_date=normalized_end,
            adjust=adjust,
        )
        path = self.store.save(frame, market, symbol, normalized_end)
        return frame, str(path)

"""Market day spot service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.data.models import MarketDaySpotRequest, normalize_trade_date
from src.data.storage import MarketDaySpotStore

if TYPE_CHECKING:
    from src.data.providers.market_day_spot import AkshareMarketDaySpotProvider

logger = logging.getLogger(__name__)


class MarketDaySpotService:
    def __init__(
        self,
        provider: "AkshareMarketDaySpotProvider | None" = None,
        store: MarketDaySpotStore | None = None,
    ) -> None:
        self.provider = provider
        self.store = store or MarketDaySpotStore()

    def _provider(self):
        if self.provider is None:
            from src.data.providers.market_day_spot import AkshareMarketDaySpotProvider

            self.provider = AkshareMarketDaySpotProvider()
        return self.provider

    def _fetch_market_day_spot(self, market: str, *, trade_date: str):
        normalized_date = normalize_trade_date(trade_date)
        return self._provider().fetch_market_day_spot(
            MarketDaySpotRequest(market=market, trade_date=normalized_date)  # type: ignore[arg-type]
        )

    def store_market_day_spot(self, market: str, *, trade_date: str):
        normalized_date = normalize_trade_date(trade_date)
        frame = self._fetch_market_day_spot(market, trade_date=normalized_date)
        path = self.store.save(frame, market, normalized_date)
        sample_api_time_field = frame.attrs.get("sample_api_time_field")
        sample_api_time_value = frame.attrs.get("sample_api_time_value")
        logger.info(
            "market_day_spot saved: market=%s trade_date=%s symbols=%s sample_api_time_field=%s sample_api_time_value=%s path=%s",
            market,
            normalized_date,
            len(frame),
            sample_api_time_field,
            sample_api_time_value,
            path,
        )
        return frame, str(path)

    def load_market_day_spot(self, market: str, *, trade_date: str):
        normalized_date = normalize_trade_date(trade_date)
        frame, path = self.store.load(market, normalized_date)
        return frame, str(path)

    def load_previous_market_day_spot(self, market: str, *, trade_date: str):
        normalized_date = normalize_trade_date(trade_date)
        result = self.store.load_previous(market, normalized_date)
        if result is None:
            return None
        frame, path = result
        return frame, str(path)

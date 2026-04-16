"""Public data-layer API."""

from src.data.services.market_data_sync_service import MarketDataSyncService


def sync_market_data(market: str, *, trade_date: str, **kwargs):
    service = MarketDataSyncService()
    return service.sync_market_data(market, trade_date=trade_date, **kwargs)


__all__ = ["sync_market_data"]

"""Public data-layer API."""

from src.data.services import HistoryKlineService, KlineSyncService, SnapshotService

_snapshot_service = SnapshotService()
_history_service = HistoryKlineService()
_sync_service = KlineSyncService()


def fetch_market_snapshot(market: str, **kwargs):
    return _snapshot_service.fetch_market_snapshot(market, **kwargs)


def fetch_and_store_snapshot(market: str, **kwargs):
    return _snapshot_service.fetch_and_store_snapshot(market, **kwargs)


def fetch_symbol_history_2y(market: str, symbol: str, **kwargs):
    return _history_service.fetch_symbol_history_2y(market, symbol, **kwargs)


def fetch_and_store_symbol_history_2y(market: str, symbol: str, **kwargs):
    return _history_service.fetch_and_store_symbol_history_2y(market, symbol, **kwargs)


def sync_latest_market_data(market: str, **kwargs):
    return _sync_service.sync_latest_market_data(market, **kwargs)


def backfill_history_batch(market: str, **kwargs):
    return _sync_service.backfill_history_batch(market, **kwargs)


__all__ = [
    "HistoryKlineService",
    "KlineSyncService",
    "SnapshotService",
    "fetch_market_snapshot",
    "fetch_and_store_snapshot",
    "fetch_symbol_history_2y",
    "fetch_and_store_symbol_history_2y",
    "sync_latest_market_data",
    "backfill_history_batch",
]

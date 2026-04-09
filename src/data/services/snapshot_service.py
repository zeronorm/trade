"""Snapshot service."""

from __future__ import annotations

from src.data.models import SnapshotRequest, normalize_trade_date
from src.data.providers import SinaSnapshotProvider
from src.data.storage import SnapshotFileStore


class SnapshotService:
    def __init__(
        self,
        provider: SinaSnapshotProvider | None = None,
        store: SnapshotFileStore | None = None,
    ) -> None:
        self.provider = provider or SinaSnapshotProvider()
        self.store = store or SnapshotFileStore()

    def fetch_market_snapshot(self, market: str):
        return self.provider.fetch_snapshot(SnapshotRequest(market=market))  # type: ignore[arg-type]

    def fetch_and_store_snapshot(self, market: str, *, trade_date: str):
        normalized_date = normalize_trade_date(trade_date)
        frame = self.fetch_market_snapshot(market)
        path = self.store.save(frame, market, normalized_date)
        return frame, str(path)

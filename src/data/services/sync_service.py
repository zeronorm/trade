"""Latest-first sync service."""

from __future__ import annotations

import logging
import random
import time

import pandas as pd

from src.data.models import SyncState, history_start_date, normalize_trade_date
from src.data.providers import SnapshotProviderError
from src.data.services.history_service import HistoryKlineService
from src.data.services.snapshot_service import SnapshotService
from src.data.storage import LatestDailyStore, SnapshotFileStore, SyncStateStore

try:
    from tqdm.auto import tqdm
except ImportError:  # pragma: no cover
    tqdm = None


logger = logging.getLogger(__name__)


def _normalize_universe_symbol(market: str, symbol: str) -> str:
    text = str(symbol).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if market == "a":
        lowered = text.lower()
        if lowered.startswith(("sh", "sz", "bj")):
            return lowered[2:]
        return lowered
    if market == "hk":
        return text.zfill(5)
    return text


def _normalize_a_provider_symbol(symbol: str) -> str:
    text = _normalize_universe_symbol("a", symbol)
    if text.startswith(("4", "8", "92")):
        return f"bj{text}"
    if text.startswith(("5", "6", "9")):
        return f"sh{text}"
    return f"sz{text}"


class KlineSyncService:
    def __init__(
        self,
        snapshot_service: SnapshotService | None = None,
        history_service: HistoryKlineService | None = None,
        snapshot_store: SnapshotFileStore | None = None,
        latest_store: LatestDailyStore | None = None,
        state_store: SyncStateStore | None = None,
    ) -> None:
        self.snapshot_service = snapshot_service or SnapshotService()
        self.history_service = history_service or HistoryKlineService()
        self.snapshot_store = snapshot_store or SnapshotFileStore()
        self.latest_store = latest_store or LatestDailyStore()
        self.state_store = state_store or SyncStateStore()

    def _load_snapshot(self, market: str, trade_date: str | None = None) -> tuple[pd.DataFrame, str]:
        frame, path = self.snapshot_store.load(market, trade_date)
        return frame, str(path)

    def _build_universe(self, market: str, frame: pd.DataFrame) -> list[dict]:
        if market == "a":
            universe = [
                {
                    "symbol": _normalize_universe_symbol(market, row["symbol"]),
                    "provider_symbol": _normalize_a_provider_symbol(str(row["provider_symbol"])),
                }
                for _, row in frame.iterrows()
            ]
            universe.sort(
                key=lambda item: (
                    0 if item["provider_symbol"].startswith("sh") else 1 if item["provider_symbol"].startswith("sz") else 2,
                    item["provider_symbol"],
                )
            )
            return universe
        return [
            {
                "symbol": _normalize_universe_symbol(market, row["symbol"]),
                "provider_symbol": _normalize_universe_symbol(market, row["provider_symbol"]),
            }
            for _, row in frame.iterrows()
        ]

    def _snapshot_to_latest_daily(self, frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        latest = frame.copy()
        latest["trade_date"] = trade_date
        latest["source"] = "sina_snapshot"
        latest["outstanding_share"] = pd.NA
        latest = latest.rename(columns={"close": "close"})
        latest = latest[
            [
                "market",
                "symbol",
                "provider_symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "turnover_rate",
                "outstanding_share",
                "source",
            ]
        ]
        return latest

    def sync_latest_market_data(self, market: str, *, trade_date: str):
        normalized_date = normalize_trade_date(trade_date)
        try:
            frame, snapshot_path = self.snapshot_service.fetch_and_store_snapshot(market, trade_date=normalized_date)
        except SnapshotProviderError as exc:
            logger.warning("latest snapshot fetch failed, fallback to local cache: market=%s error=%s", market, exc)
            try:
                frame, cached_path = self._load_snapshot(market, None)
            except FileNotFoundError as cache_exc:
                raise RuntimeError(
                    f"latest snapshot fetch failed for market={market}, and no local cached snapshot is available"
                ) from cache_exc
            snapshot_path = cached_path
        latest_frame = self._snapshot_to_latest_daily(frame, normalized_date)
        latest_path = self.latest_store.save(latest_frame, market, normalized_date)
        universe = self._build_universe(market, frame)
        existing = self.state_store.load(market)  # type: ignore[arg-type]
        current_symbols = {item["symbol"] for item in universe}
        completed = tuple(sorted(symbol for symbol in (existing.completed_symbols if existing else ()) if symbol in current_symbols))
        failed = tuple(sorted(symbol for symbol in (existing.failed_symbols if existing else ()) if symbol in current_symbols))
        state = SyncState(
            market=market,  # type: ignore[arg-type]
            latest_trade_date=normalized_date,
            snapshot_path=snapshot_path,
            latest_daily_path=str(latest_path),
            history_target_start_date=history_start_date(normalized_date),
            total_symbols=len(universe),
            completed_symbols=completed,
            failed_symbols=failed,
            history_complete=len(completed) >= len(universe) and len(universe) > 0,
        )
        state_path = self.state_store.save(state)
        return {
            "market": market,
            "trade_date": normalized_date,
            "snapshot_path": snapshot_path,
            "latest_daily_path": str(latest_path),
            "state_path": str(state_path),
            "rows": len(latest_frame),
            "symbols": len(universe),
        }

    def backfill_history_batch(
        self,
        market: str,
        *,
        batch_size: int,
        trade_date: str | None = None,
        continue_on_error: bool = True,
        pause_seconds: float = 0.0,
        pause_jitter_seconds: float = 0.0,
        retries: int = 3,
        retry_pause_seconds: float = 0.5,
    ):
        state = self.state_store.load(market)  # type: ignore[arg-type]
        if state is None:
            if trade_date is None:
                raise ValueError("trade_date is required when sync state is missing")
            self.sync_latest_market_data(market, trade_date=trade_date)
            state = self.state_store.load(market)  # type: ignore[arg-type]
        assert state is not None

        snapshot_frame, _ = self._load_snapshot(market, state.latest_trade_date)
        universe = self._build_universe(market, snapshot_frame)
        completed = set(state.completed_symbols)
        failed = set(state.failed_symbols)
        pending = [item for item in universe if item["symbol"] not in completed]
        batch = pending[:batch_size]

        logger.info(
            "start history backfill: market=%s latest_trade_date=%s pending=%s batch=%s",
            market,
            state.latest_trade_date,
            len(pending),
            len(batch),
        )
        iterator = batch
        if tqdm is not None:
            iterator = tqdm(batch, total=len(batch), desc=f"{market} history", unit="symbol")

        success_count = 0
        error_count = 0
        for item in iterator:
            symbol = item["symbol"]
            provider_symbol = item["provider_symbol"]
            last_exception: Exception | None = None
            for attempt in range(1, max(retries, 1) + 1):
                try:
                    self.history_service.fetch_and_store_symbol_history_2y(
                        market,
                        symbol,
                        provider_symbol=provider_symbol,
                        end_date=state.latest_trade_date,
                        skip_existing=True,
                    )
                    completed.add(symbol)
                    failed.discard(symbol)
                    success_count += 1
                    last_exception = None
                    break
                except Exception as exc:
                    last_exception = exc
                    if attempt < max(retries, 1):
                        logger.warning(
                            "history retry: market=%s symbol=%s provider_symbol=%s attempt=%s/%s error=%s",
                            market,
                            symbol,
                            provider_symbol,
                            attempt,
                            max(retries, 1),
                            exc,
                        )
                        time.sleep(retry_pause_seconds)
            if last_exception is not None:
                error_count += 1
                failed.add(symbol)
                logger.warning(
                    "history fetch failed: market=%s symbol=%s provider_symbol=%s error=%s",
                    market,
                    symbol,
                    provider_symbol,
                    last_exception,
                )
                if not continue_on_error:
                    raise last_exception
            if pause_seconds > 0 or pause_jitter_seconds > 0:
                time.sleep(pause_seconds + random.uniform(0.0, pause_jitter_seconds))

            updated_state = SyncState(
                market=state.market,
                provider=state.provider,
                latest_trade_date=state.latest_trade_date,
                snapshot_path=state.snapshot_path,
                latest_daily_path=state.latest_daily_path,
                history_target_start_date=state.history_target_start_date,
                total_symbols=len(universe),
                completed_symbols=tuple(sorted(completed)),
                failed_symbols=tuple(sorted(failed)),
                history_complete=len(completed) >= len(universe) and len(universe) > 0,
            )
            self.state_store.save(updated_state)
            state = updated_state

        remaining = max(len(universe) - len(completed), 0)
        return {
            "market": market,
            "latest_trade_date": state.latest_trade_date,
            "success": success_count,
            "errors": error_count,
            "remaining": remaining,
            "history_complete": state.history_complete,
            "state_path": str(self.state_store.build_path(market)),
        }

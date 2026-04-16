"""Single-symbol history sync service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from src.data.models import (
    SymbolHistRequest,
    build_hist_key,
    history_start_date,
    normalize_display_symbol,
    normalize_optional_text,
    normalize_provider_symbol,
    normalize_trade_date,
)
from src.data.services.market_day_spot_service import MarketDaySpotService
from src.data.storage import SymbolHistStore

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from src.data.providers.symbol_hist import AkshareSymbolHistProvider

try:
    from tqdm.auto import tqdm
except ImportError:  # pragma: no cover
    tqdm = None


class SymbolHistService:
    def __init__(
        self,
        provider: "AkshareSymbolHistProvider | None" = None,
        spot_service: MarketDaySpotService | None = None,
        store: SymbolHistStore | None = None,
    ) -> None:
        self.provider = provider
        self.spot_service = spot_service or MarketDaySpotService()
        self.store = store or SymbolHistStore()

    def _provider(self):
        if self.provider is None:
            from src.data.providers.symbol_hist import AkshareSymbolHistProvider

            self.provider = AkshareSymbolHistProvider()
        return self.provider

    def _fetch_symbol_hist(
        self,
        market: str,
        board: str,
        symbol: str,
        *,
        trade_date: str,
        provider_symbol: str = "",
        start_date: str | None = None,
        end_date: str | None = None,
        adjust: str = "qfq",
    ):
        normalized_trade_date = normalize_trade_date(trade_date)
        resolved_start_date = start_date or history_start_date(normalized_trade_date)
        resolved_end_date = end_date or normalized_trade_date
        return self._provider().fetch_symbol_hist(
            SymbolHistRequest(
                market=market,  # type: ignore[arg-type]
                board=board,
                symbol=symbol,
                provider_symbol=provider_symbol,
                trade_date=normalized_trade_date,
                start_date=resolved_start_date,
                end_date=resolved_end_date,
                adjust=adjust,
            )
        )

    def diff_new_symbols(self, market: str, *, trade_date: str):
        current_frame, current_path = self.spot_service.load_market_day_spot(market, trade_date=trade_date)
        previous = self.spot_service.load_previous_market_day_spot(market, trade_date=trade_date)
        if previous is None:
            new_frame = current_frame.copy()
            previous_path = None
        else:
            previous_frame, previous_path = previous
            current_keys = current_frame.apply(lambda row: build_hist_key(row["board"], row["symbol"]), axis=1)
            previous_keys = set(
                previous_frame.apply(lambda row: build_hist_key(row["board"], row["symbol"]), axis=1).tolist()
            )
            new_frame = current_frame.loc[~current_keys.isin(previous_keys)].copy()
        new_frame.sort_values(by=["board", "symbol"], inplace=True, ignore_index=True)
        return new_frame, current_path, previous_path

    def sync_new_symbol_hist(
        self,
        market: str,
        *,
        trade_date: str,
        limit: int | None = None,
        adjust: str = "qfq",
        continue_on_error: bool = True,
    ):
        new_symbols, current_path, previous_path = self.diff_new_symbols(market, trade_date=trade_date)
        if limit is not None:
            new_symbols = new_symbols.head(limit).copy()
        logger.info(
            "sync new symbol_hist: market=%s trade_date=%s current=%s previous=%s new_symbols=%s",
            market,
            trade_date,
            current_path,
            previous_path,
            len(new_symbols),
        )
        success = 0
        errors = 0
        skipped = 0
        hist_path = str(self.store.build_path(market))
        existing_frame, _ = self.store.load_or_empty(market)
        existing_keys: set[tuple[str, str, str]] = set()
        if not existing_frame.empty:
            for row in existing_frame.itertuples(index=False):
                existing_keys.add(
                    (
                        normalize_optional_text(getattr(row, "board", "")),
                        normalize_optional_text(getattr(row, "symbol", "")),
                        normalize_optional_text(getattr(row, "trade_date", "")),
                    )
                )
        pending_frames = []
        normalized_trade_date = normalize_trade_date(trade_date)
        sample_trade_date = None

        def flush_pending() -> tuple[pd.DataFrame, str]:
            nonlocal pending_frames
            non_empty_frames = [frame for frame in pending_frames if frame is not None and not frame.empty]
            if not non_empty_frames:
                return existing_frame, hist_path
            frames_to_concat = ([existing_frame] if not existing_frame.empty else []) + non_empty_frames
            merged_input = pd.concat(frames_to_concat, ignore_index=True)
            path = str(self.store.write(merged_input, market))
            pending_frames = []
            return merged_input, path

        iterator = new_symbols.itertuples(index=False)
        progress = None
        if tqdm is not None:
            progress = tqdm(
                iterator,
                total=len(new_symbols),
                desc=f"{market} symbol_hist",
                unit="symbol",
                dynamic_ncols=True,
            )
            iterator = progress
        try:
            for row in iterator:
                board = normalize_optional_text(row.board)
                symbol = normalize_display_symbol(market, row.symbol)
                provider_symbol = normalize_provider_symbol(market, board, symbol, row.provider_symbol)
                try:
                    key = (board, symbol, normalized_trade_date)
                    if key in existing_keys:
                        skipped += 1
                        if progress is not None:
                            progress.set_postfix_str(f"symbol={symbol} succ={success} skip={skipped} fail={errors}")
                        continue
                    frame = self._fetch_symbol_hist(
                        market,
                        board,
                        symbol,
                        trade_date=trade_date,
                        provider_symbol=provider_symbol,
                        adjust=adjust,
                    )
                    pending_frames.append(frame)
                    if sample_trade_date is None and frame is not None and not frame.empty:
                        sample_trade_date = str(frame.iloc[0].get("trade_date", ""))
                    for hist_row in frame.itertuples(index=False):
                        existing_keys.add(
                            (
                                normalize_optional_text(getattr(hist_row, "board", "")),
                                normalize_optional_text(getattr(hist_row, "symbol", "")),
                                normalize_optional_text(getattr(hist_row, "trade_date", "")),
                            )
                        )
                    success += 1
                    if progress is not None:
                        progress.set_postfix_str(f"symbol={symbol} succ={success} skip={skipped} fail={errors}")
                except Exception as exc:
                    errors += 1
                    if progress is not None:
                        progress.set_postfix_str(f"symbol={symbol} succ={success} skip={skipped} fail={errors}")
                    logger.warning(
                        "symbol_hist fetch failed: market=%s board=%s symbol=%s provider_symbol=%s error=%s",
                        market,
                        board,
                        symbol,
                        provider_symbol,
                        exc,
                    )
                    if not continue_on_error:
                        raise
        except KeyboardInterrupt:
            existing_frame, hist_path = flush_pending()
            logger.warning(
                "symbol_hist interrupted: market=%s trade_date=%s new_symbols=%s success=%s skipped=%s errors=%s sample_trade_date=%s hist_path=%s",
                market,
                normalized_trade_date,
                len(new_symbols),
                success,
                skipped,
                errors,
                sample_trade_date,
                hist_path,
            )
            return {
                "market": market,
                "trade_date": normalized_trade_date,
                "current_day_path": current_path,
                "previous_day_path": previous_path,
                "new_symbols": len(new_symbols),
                "success": success,
                "skipped": skipped,
                "errors": errors,
                "sample_trade_date": sample_trade_date,
                "hist_path": hist_path,
                "interrupted": True,
            }
        existing_frame, hist_path = flush_pending()
        logger.info(
            "symbol_hist synced: market=%s trade_date=%s new_symbols=%s success=%s skipped=%s errors=%s sample_trade_date=%s hist_path=%s",
            market,
            normalized_trade_date,
            len(new_symbols),
            success,
            skipped,
            errors,
            sample_trade_date,
            hist_path,
        )
        return {
            "market": market,
            "trade_date": normalized_trade_date,
            "current_day_path": current_path,
            "previous_day_path": previous_path,
            "new_symbols": len(new_symbols),
            "success": success,
            "skipped": skipped,
            "errors": errors,
            "sample_trade_date": sample_trade_date,
            "hist_path": hist_path,
            "interrupted": False,
        }

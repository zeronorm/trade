"""Orchestrate data sync: spot → full hist → merged hist per market."""

from __future__ import annotations

import logging
import time

import pandas as pd
from tqdm.auto import tqdm

from src.data.models import HIST_COLUMNS, clean, normalize_trade_date, provider_symbol
from src.data.fetch import fetch_spot, fetch_hist
from src.data import store

logger = logging.getLogger(__name__)

_HIST_NUMERIC_COLUMNS = [
    "open", "close", "high", "low", "volume", "amount",
    "amplitude", "change_pct", "change", "turnover_rate",
]


def _symbol_keys(frame: pd.DataFrame) -> set[tuple[str, str]]:
    if frame.empty:
        return set()
    return {
        (clean(board), clean(symbol))
        for board, symbol in zip(frame["board"].fillna(""), frame["symbol"].fillna(""))
    }


def _build_hist_frame(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=HIST_COLUMNS)

    merged = pd.concat(frames, ignore_index=True)
    merged.sort_values(by=["board", "symbol", "trade_date"], inplace=True, ignore_index=True)
    for col in _HIST_NUMERIC_COLUMNS:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")
    return merged.reindex(columns=HIST_COLUMNS)


def _fetch_hist_with_retry(
    market: str,
    board: str,
    sym: str,
    *,
    trade_date: str,
    provider_sym: str,
    adjust: str,
    retries: int,
    retry_delay: float,
) -> pd.DataFrame:
    attempts = retries + 1
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return fetch_hist(
                market,
                board,
                sym,
                trade_date=trade_date,
                provider_sym=provider_sym,
                adjust=adjust,
            )
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            logger.warning(
                "hist retry: market=%s board=%s symbol=%s provider=%s attempt=%d/%d error=%s(%r)",
                market,
                board or "-",
                sym,
                provider_sym,
                attempt,
                attempts,
                exc.__class__.__name__,
                exc,
            )
            if retry_delay > 0:
                time.sleep(retry_delay)

    assert last_error is not None
    raise last_error


def sync_market_data(
    market: str,
    *,
    trade_date: str,
    limit: int | None = None,
    adjust: str = "qfq",
    continue_on_error: bool = True,
    hist_retries: int = 2,
    hist_retry_delay: float = 1.0,
) -> dict:
    trade_date = normalize_trade_date(trade_date)

    # --- 1. spot (全市场快照) ---
    if store.day_exists(market, trade_date):
        logger.info("spot cache hit: %s %s", market, trade_date)
        spot = store.load_day(market, trade_date)
    else:
        logger.info("spot fetching: %s %s", market, trade_date)
        spot = fetch_spot(market, trade_date)
        store.save_day(spot, market, trade_date)

    # --- 2. hist symbols (当日全量股票) ---
    hist_symbols = spot.copy()
    hist_symbols.sort_values(by=["board", "symbol"], inplace=True, ignore_index=True)
    if limit is not None:
        hist_symbols = hist_symbols.head(limit)
    hist_total_symbols = len(hist_symbols)

    existing_hist = store.compact_hist(market, trade_date) if store.hist_exists(market, trade_date) else pd.DataFrame(columns=HIST_COLUMNS)
    existing_progress = store.load_hist_progress(market, trade_date)
    completed_keys = _symbol_keys(existing_hist) | _symbol_keys(existing_progress)

    pending_symbols = hist_symbols[
        ~hist_symbols.apply(lambda row: (clean(row.get("board", "")), clean(row["symbol"])) in completed_keys, axis=1)
    ].copy()

    logger.info(
        "hist progress: total=%d completed=%d pending=%d",
        hist_total_symbols,
        len(completed_keys),
        len(pending_symbols),
    )

    # --- 3. hist cache ---
    if pending_symbols.empty and store.hist_exists(market, trade_date):
        logger.info("hist cache hit: %s %s", market, trade_date)
        hist_out = str(store.hist_path(market, trade_date))
        if store.hist_merge_exists(market, trade_date):
            merge_out = str(store.hist_merge_path(market, trade_date))
        else:
            merge_out = str(store.merge_hist(market, trade_date, current_frame=existing_hist))
        return {
            "market": market, "trade_date": trade_date,
            "day_rows": len(spot),
            "day_path": str(store.day_path(market, trade_date)),
            "hist_total_symbols": hist_total_symbols,
            "symbols_to_fetch": 0,
            "hist_success": 0, "hist_errors": 0,
            "hist_cached": True,
            "hist_path": hist_out,
            "merge_path": merge_out,
        }

    # --- 4. hist (逐只拉历史) ---
    success = errors = 0

    progress = tqdm(
        pending_symbols.itertuples(index=False),
        total=len(pending_symbols),
        desc=f"{market} hist ({trade_date})",
        unit="sym",
        dynamic_ncols=True,
    )

    for row in progress:
        board = clean(row.board)
        sym = str(row.symbol)
        psym = provider_symbol(market, board, sym, getattr(row, "provider_symbol", ""))

        try:
            frame = _build_hist_frame([
                _fetch_hist_with_retry(
                    market,
                    board,
                    sym,
                    trade_date=trade_date,
                    provider_sym=psym,
                    adjust=adjust,
                    retries=hist_retries,
                    retry_delay=hist_retry_delay,
                )
            ])
            if not frame.empty:
                store.append_hist(frame, market, trade_date)
            store.append_hist_progress(
                market,
                trade_date,
                board=board,
                symbol=sym,
                provider_symbol=psym,
                has_data=not frame.empty,
            )
            success += 1
        except Exception as exc:
            errors += 1
            logger.warning(
                "hist failed: market=%s board=%s symbol=%s provider=%s error=%s(%r)",
                market,
                board or "-",
                sym,
                psym,
                exc.__class__.__name__,
                exc,
            )
            if not continue_on_error:
                raise
        progress.set_postfix_str(f"sym={sym} ok={success} err={errors}")

    # --- 5. save hist ---
    if store.hist_exists(market, trade_date):
        hist_frame = store.compact_hist(market, trade_date)
    else:
        hist_frame = pd.DataFrame(columns=HIST_COLUMNS)
        store.save_hist(hist_frame, market, trade_date)
    hist_out = str(store.hist_path(market, trade_date))

    merge_out = str(store.merge_hist(market, trade_date, current_frame=hist_frame))

    return {
        "market": market, "trade_date": trade_date,
        "day_rows": len(spot),
        "day_path": str(store.day_path(market, trade_date)),
        "hist_total_symbols": hist_total_symbols,
        "symbols_to_fetch": len(pending_symbols),
        "hist_success": success, "hist_errors": errors,
        "hist_cached": False,
        "hist_path": hist_out,
        "merge_path": merge_out,
    }

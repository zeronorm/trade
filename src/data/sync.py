"""Orchestrate data sync: spot → hist per market."""

from __future__ import annotations

import logging

import pandas as pd
from tqdm.auto import tqdm

from src.data.models import HIST_COLUMNS, clean, normalize_trade_date, provider_symbol
from src.data.fetch import fetch_spot, fetch_hist
from src.data import store

logger = logging.getLogger(__name__)


def sync_market_data(
    market: str,
    *,
    trade_date: str,
    limit: int | None = None,
    adjust: str = "qfq",
    continue_on_error: bool = True,
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

    # --- 2. diff new symbols ---
    prev = store.load_prev_day(market, trade_date)
    if prev is None:
        new_symbols = spot.copy()
    else:
        cur_keys = set(zip(spot["board"].fillna(""), spot["symbol"].fillna("")))
        prev_keys = set(zip(prev["board"].fillna(""), prev["symbol"].fillna("")))
        new_keys = cur_keys - prev_keys
        new_symbols = spot[spot.apply(
            lambda r: (clean(r.get("board", "")), clean(r["symbol"])) in new_keys, axis=1
        )].copy()

    new_symbols.sort_values(by=["board", "symbol"], inplace=True, ignore_index=True)
    if limit is not None:
        new_symbols = new_symbols.head(limit)

    logger.info("new symbols to fetch: %d", len(new_symbols))

    # --- 3. hist cache ---
    if store.hist_exists(market, trade_date):
        logger.info("hist cache hit: %s %s", market, trade_date)
        return {
            "market": market, "trade_date": trade_date,
            "day_rows": len(spot),
            "day_path": str(store.day_path(market, trade_date)),
            "new_symbols": len(new_symbols),
            "hist_success": 0, "hist_skipped": len(new_symbols), "hist_errors": 0,
            "hist_path": str(store.hist_path(market, trade_date)),
        }

    # --- 4. hist (逐只拉历史) ---
    frames: list[pd.DataFrame] = []
    success = errors = 0

    progress = tqdm(
        new_symbols.itertuples(index=False),
        total=len(new_symbols),
        desc=f"{market} hist ({trade_date})",
        unit="sym",
        dynamic_ncols=True,
    )

    for row in progress:
        board = clean(row.board)
        sym = str(row.symbol)
        psym = provider_symbol(market, board, sym, getattr(row, "provider_symbol", ""))

        try:
            frame = fetch_hist(market, board, sym, trade_date=trade_date, provider_sym=psym, adjust=adjust)
            if frame is not None and not frame.empty:
                frames.append(frame)
            success += 1
        except Exception as exc:
            errors += 1
            logger.warning("hist failed: %s %s %s — %s", market, board, sym, exc)
            if not continue_on_error:
                raise
        progress.set_postfix_str(f"sym={sym} ok={success} err={errors}")

    # --- 5. save hist ---
    if frames:
        merged = pd.concat(frames, ignore_index=True)
        merged.sort_values(by=["board", "symbol", "trade_date"], inplace=True, ignore_index=True)
        for col in ["open", "close", "high", "low", "volume", "amount",
                     "amplitude", "change_pct", "change", "turnover_rate"]:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")
        merged = merged[HIST_COLUMNS]
        hist_out = str(store.save_hist(merged, market, trade_date))
    else:
        hist_out = str(store.hist_path(market, trade_date))

    return {
        "market": market, "trade_date": trade_date,
        "day_rows": len(spot),
        "day_path": str(store.day_path(market, trade_date)),
        "new_symbols": len(new_symbols),
        "hist_success": success, "hist_skipped": 0, "hist_errors": errors,
        "hist_path": hist_out,
    }

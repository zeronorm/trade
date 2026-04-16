"""Orchestrate per-market day sync in one execution window."""

from __future__ import annotations

import logging

from src.data.models import normalize_trade_date
from src.data.services.market_day_spot_service import MarketDaySpotService
from src.data.services.symbol_hist_service import SymbolHistService

logger = logging.getLogger(__name__)


class MarketDataSyncService:
    def __init__(
        self,
        spot_service: MarketDaySpotService | None = None,
        hist_service: SymbolHistService | None = None,
    ) -> None:
        self.spot_service = spot_service or MarketDaySpotService()
        self.hist_service = hist_service or SymbolHistService(spot_service=self.spot_service)

    def sync_market_data(
        self,
        market: str,
        *,
        trade_date: str,
        limit: int | None = None,
        adjust: str = "qfq",
        continue_on_error: bool = True,
    ) -> dict[str, object]:
        normalized_trade_date = normalize_trade_date(trade_date)
        day_frame, day_path = self.spot_service.store_market_day_spot(market, trade_date=normalized_trade_date)
        day_rows = len(day_frame)
        day_api_time_field = day_frame.attrs.get("sample_api_time_field")
        day_api_time_value = day_frame.attrs.get("sample_api_time_value")
        hist_result = self.hist_service.sync_new_symbol_hist(
            market,
            trade_date=normalized_trade_date,
            limit=limit,
            adjust=adjust,
            continue_on_error=continue_on_error,
        )
        return {
            "market": market,
            "trade_date": normalized_trade_date,
            "day_rows": day_rows,
            "day_api_time_field": day_api_time_field,
            "day_api_time_value": day_api_time_value,
            "day_path": day_path,
            "hist_new_symbols": hist_result["new_symbols"],
            "hist_success": hist_result["success"],
            "hist_skipped": hist_result["skipped"],
            "hist_errors": hist_result["errors"],
            "hist_sample_trade_date": hist_result["sample_trade_date"],
            "hist_path": hist_result["hist_path"],
            "interrupted": hist_result["interrupted"],
        }

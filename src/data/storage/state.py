"""JSON state storage for latest-first sync."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from src.data.models import SyncState


class SyncStateStore:
    def __init__(self, root: str | Path = "data_store/metadata/kline_sync") -> None:
        self.root = Path(root)

    def build_path(self, market: str, provider: str = "sina") -> Path:
        return self.root / provider / f"{market}.json"

    def load(self, market: str, provider: str = "sina") -> SyncState | None:
        path = self.build_path(market, provider)
        if not path.exists():
            return None
        payload = json.loads(path.read_text())
        return SyncState(
            market=payload["market"],
            provider=payload.get("provider", provider),
            latest_trade_date=payload.get("latest_trade_date", ""),
            snapshot_path=payload.get("snapshot_path", ""),
            latest_daily_path=payload.get("latest_daily_path", ""),
            history_target_start_date=payload.get("history_target_start_date", ""),
            total_symbols=payload.get("total_symbols", 0),
            completed_symbols=tuple(payload.get("completed_symbols", [])),
            failed_symbols=tuple(payload.get("failed_symbols", [])),
            history_complete=payload.get("history_complete", False),
        )

    def save(self, state: SyncState) -> Path:
        path = self.build_path(state.market, state.provider)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(state)
        payload["completed_symbols"] = list(state.completed_symbols)
        payload["failed_symbols"] = list(state.failed_symbols)
        payload["updated_at"] = datetime.utcnow().isoformat()
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        return path

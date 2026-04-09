from src.data.storage import HistoryFileStore, LatestDailyStore, SnapshotFileStore, SyncStateStore


def test_storage_paths(tmp_path) -> None:
    snapshot_store = SnapshotFileStore(tmp_path / "snapshots")
    latest_store = LatestDailyStore(tmp_path / "daily_latest")
    history_store = HistoryFileStore(tmp_path / "history_2y")
    state_store = SyncStateStore(tmp_path / "metadata")

    assert snapshot_store.build_path("a", "2026-04-09").name == "a.2026-04-09.csv"
    assert latest_store.build_path("us", "2026-04-09").name == "us.2026-04-09.csv"
    assert history_store.build_path("hk", "00700", "2026-04-09").name == "00700.2026-04-09.history_2y.csv"
    assert state_store.build_path("a").name == "a.json"

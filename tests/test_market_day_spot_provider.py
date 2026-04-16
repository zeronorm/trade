import pandas as pd

from src.data.providers.market_day_spot import _normalize_cn, _normalize_hk, _normalize_numeric, _normalize_us


def test_cn_api_time_attrs() -> None:
    raw = pd.DataFrame(
        [
            {
                "代码": "sz000001",
                "名称": "平安银行",
                "今开": 10.0,
                "最新价": 10.2,
                "最高": 10.3,
                "最低": 9.9,
                "成交量": 100,
                "成交额": 1000,
                "时间戳": "15:30:00",
            }
        ]
    )
    frame = _normalize_numeric(_normalize_cn(raw, "2026-04-14"))
    assert frame.attrs["sample_api_time_field"] == "时间戳"
    assert frame.attrs["sample_api_time_value"] == "15:30:00"


def test_hk_api_time_attrs() -> None:
    raw = pd.DataFrame(
        [
            {
                "代码": "00001",
                "中文名称": "长和",
                "英文名称": "CKH HOLDINGS",
                "今开": 64.3,
                "最新价": 64.05,
                "最高": 64.6,
                "最低": 63.65,
                "成交量": 1,
                "成交额": 2,
                "日期时间": "2026/04/14 16:08:24",
            }
        ]
    )
    frame = _normalize_numeric(_normalize_hk(raw, "2026-04-14"))
    assert frame.attrs["sample_api_time_field"] == "日期时间"
    assert frame.attrs["sample_api_time_value"] == "2026/04/14 16:08:24"


def test_us_api_time_attrs_absent() -> None:
    raw = pd.DataFrame(
        [
            {
                "symbol": "NVDA",
                "name": "NVIDIA Corp.",
                "cname": "英伟达公司",
                "open": "190.84",
                "price": "194.28",
                "high": "194.39",
                "low": "190.77",
                "volume": "71754802",
            }
        ]
    )
    frame = _normalize_numeric(_normalize_us(raw, "2026-04-14"))
    assert frame.attrs["sample_api_time_field"] is None
    assert frame.attrs["sample_api_time_value"] is None

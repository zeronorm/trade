# trade

`data` 层当前按“单 market 单次执行”组织，统一入口是 `market_data`：

1. 先落当天 `market_day_spot`
2. 再在同一次执行里补 `symbol_hist`

目录仍按 `AGENTS.md` 规划保留，当前已实现的核心代码在 [src/data](/Users/yugh/YUGH/dev/trade/src/data)。

## 流程

### Unified Process: `market_data`

- 按市场抓取某个交易日的全市场横截面
- 同一次执行中继续同步该市场新增 symbol 的两年历史
- 使用：
  - A 股：`ak.stock_zh_a_spot()`
  - 港股：`ak.stock_hk_spot()`
  - 美股：`ak.stock_us_spot()`
- 打平成统一 schema 后保存到：
  - `data_store/day/{market}.{trade_date}.csv`
- 再读取同市场上一份 `market_day_spot` 做 diff
- 只为新增 symbol 拉两年历史并汇总到：
  - `data_store/hist/{market}.csv`

使用：

- A 股：新浪历史 JS 接口，自定义解析实现
- 港股：`ak.stock_hk_daily(...)`
- 美股：`ak.stock_us_daily(...)`

统一列定义见 [models.py](/Users/yugh/YUGH/dev/trade/src/data/models.py)：

- `market`
- `board`
- `symbol`
- `provider_symbol`
- `trade_date`
- `open`
- `close`
- `high`
- `low`
- `volume`
- `amount`
- `amplitude`
- `change_pct`
- `change`
- `turnover_rate`
- `source`

## 代码结构

- Orchestrator:
  - [market_data_sync_service.py](/Users/yugh/YUGH/dev/trade/src/data/services/market_data_sync_service.py)
- Provider:
  - [market_day_spot.py](/Users/yugh/YUGH/dev/trade/src/data/providers/market_day_spot.py)
  - [symbol_hist.py](/Users/yugh/YUGH/dev/trade/src/data/providers/symbol_hist.py)
- Service:
  - [market_day_spot_service.py](/Users/yugh/YUGH/dev/trade/src/data/services/market_day_spot_service.py)
  - [symbol_hist_service.py](/Users/yugh/YUGH/dev/trade/src/data/services/symbol_hist_service.py)
- Storage:
  - [files.py](/Users/yugh/YUGH/dev/trade/src/data/storage/files.py)

## 脚本

### 统一执行 `market_data`

```bash
python scripts/sync_market_data.py --market all --trade-date 2026-04-10 --continue-on-error
```

这会对每个 market 顺序执行：

- `market_day_spot`
- `symbol_hist`

## 示例

```bash
python examples/run_data_demo.py
```

## 测试

```bash
python -m compileall src scripts examples tests
conda run -n dev python -m pytest -q tests/test_data_models.py tests/test_storage_paths.py tests/test_symbol_hist_service.py
```

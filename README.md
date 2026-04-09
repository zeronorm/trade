# Trade Project

当前仓库先实现 `data` 层，目标是用 `akshare.sina` 完成 A 股、港股、美股的最新横截面同步和 2 年历史日线回补。

## 目录

一级目录按 [AGENTS.md](/Users/yugh/YUGH/dev/trade/AGENTS.md) 规划：

- `src/core`: 核心引擎预留
- `src/data`: 市场数据层，当前已实现
- `src/strategy`: 策略层预留
- `src/risk`: 风控层预留
- `src/backtest`: 回测层预留
- `src/utils`: 通用工具预留
- `src/config`: 代码侧配置预留
- `examples`: 调用示例
- `scripts`: 定时任务和运维脚本
- `tests`: 自动化测试
- `config`: 配置文件目录
- `data_store`: 本地数据目录
- `notebooks`: 研究笔记目录

## Data 设计

当前 `data` 层只保留 `sina` 路线。

- 最新数据优先
  - 每个市场先抓全市场 snapshot
  - 再把 snapshot 直接转换成当日全量日线文件
- 历史数据异步回补
  - 按 ticker 分批补最近 2 年历史
  - 历史任务慢跑，不阻塞最新数据可用性

公开入口在 [src/data/__init__.py](/Users/yugh/YUGH/dev/trade/src/data/__init__.py)。

## 三市场统一打平格式

### Snapshot

列定义见 [src/data/models.py:12](/Users/yugh/YUGH/dev/trade/src/data/models.py:12)：

- `market`
- `symbol`
- `provider_symbol`
- `name`
- `open`
- `high`
- `low`
- `close`
- `prev_close`
- `volume`
- `amount`
- `turnover_rate`
- `source`

三市场 snapshot 归一化实现见 [src/data/providers/sina.py:31](/Users/yugh/YUGH/dev/trade/src/data/providers/sina.py:31)。

### Daily Kline

列定义见 [src/data/models.py:28](/Users/yugh/YUGH/dev/trade/src/data/models.py:28)：

- `market`
- `symbol`
- `provider_symbol`
- `trade_date`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `amount`
- `turnover_rate`
- `outstanding_share`
- `source`

三市场历史日线打平实现见 [src/data/providers/sina_kline.py:104](/Users/yugh/YUGH/dev/trade/src/data/providers/sina_kline.py:104)。

## 数据落盘格式

路径定义见 [src/data/storage/files.py](/Users/yugh/YUGH/dev/trade/src/data/storage/files.py)。

- Snapshot:
  - `data_store/snapshots/{market}.{trade_date}.csv`
  - 示例: `data_store/snapshots/a.2026-04-09.csv`
- 最新日线:
  - `data_store/daily_latest/{market}.{trade_date}.csv`
  - 示例: `data_store/daily_latest/us.2026-04-09.csv`
- 两年历史:
  - `data_store/history_2y/{market}/{symbol}.{end_date}.history_2y.csv`
  - 示例: `data_store/history_2y/hk/00700.2026-04-09.history_2y.csv`
- 同步状态:
  - `data_store/metadata/kline_sync/sina/{market}.json`

状态文件记录：

- `latest_trade_date`
- `snapshot_path`
- `latest_daily_path`
- `history_target_start_date`
- `total_symbols`
- `completed_symbols`
- `failed_symbols`
- `history_complete`

## 环境

默认使用 `conda dev` 环境。

基础检查：

```bash
conda run -n dev python -c "import akshare; print(akshare.__version__)"
```

## 首次初始化

首次初始化建议分两步。

### 1. 先同步最新日

```bash
./scripts/run_kline_sync.sh --mode latest --market a
./scripts/run_kline_sync.sh --mode latest --market hk
./scripts/run_kline_sync.sh --mode latest --market us
```

也可以一次跑完：

```bash
./scripts/run_kline_sync.sh --mode latest --market all
```

这一步会：

- 抓取全市场最新 snapshot
- 生成当日全量日线文件
- 初始化后续历史回补需要的状态文件

### 2. 再跑历史回补

```bash
./scripts/run_kline_sync.sh --mode history-backfill --market a --batch-size 300 --continue-on-error
./scripts/run_kline_sync.sh --mode history-backfill --market hk --batch-size 150 --continue-on-error
./scripts/run_kline_sync.sh --mode history-backfill --market us --batch-size 80 --continue-on-error
```

建议把这类任务当作后台慢任务反复执行，直到对应市场的状态文件里 `history_complete=true`。

## 日常运行

日常只需要保证两个目标：

1. 当日最新数据完整
2. 历史回补后台持续推进

推荐顺序：

1. 每个市场先跑一次 `latest`
2. 再跑对应市场的 `history-backfill`

## Shell 入口

统一入口是 [scripts/run_kline_sync.sh](/Users/yugh/YUGH/dev/trade/scripts/run_kline_sync.sh)。

帮助：

```bash
./scripts/run_kline_sync.sh --help
```

支持模式：

- `--mode latest`
- `--mode history-backfill`

常用参数：

- `--market a|hk|us|all`
- `--as-of-date YYYYMMDD`
- `--batch-size N`
- `--continue-on-error`
- `--pause-seconds FLOAT`
- `--pause-jitter-seconds FLOAT`
- `--retries N`
- `--retry-pause-seconds FLOAT`

## 不同市场的时间规则

机器时区按中国时区运行，但脚本内部会按不同市场计算默认交易日。

规则定义见 [scripts/run_kline_sync.sh:76](/Users/yugh/YUGH/dev/trade/scripts/run_kline_sync.sh:76)：

- A 股:
  - 时区 `Asia/Shanghai`
  - 截止时间 `15:30`
- 港股:
  - 时区 `Asia/Shanghai`
  - 截止时间 `16:30`
- 美股:
  - 时区 `America/New_York`
  - 截止时间 `17:00`

如果未到该市场的默认可用时间，脚本会自动回退到前一个自然日。

## Crontab

模板见 [crontab.example](/Users/yugh/YUGH/dev/trade/crontab.example)。

当前推荐计划基于机器位于 `Asia/Shanghai`：

- 最新数据
  - A 股: `15:40`
  - 港股: `16:40`
  - 美股: 次日 `06:20`
- 历史回补
  - A 股: `20:30`
  - 港股: `21:00`
  - 美股: 次日 `06:40`

启用前先建日志目录：

```bash
mkdir -p /root/work/trade/logs
```

然后：

```bash
crontab /root/work/trade/crontab.example
```

或手动：

```bash
crontab -e
```

## 日志和排查

日志默认写到 `logs/`。

常见检查点：

- `latest` 是否成功生成 `data_store/daily_latest/{market}.{date}.csv`
- `data_store/metadata/kline_sync/sina/{market}.json` 是否更新
- `history_complete` 是否为 `true`
- `failed_symbols` 是否持续增长

## 示例

示例脚本在 [examples/run_data_demo.py](/Users/yugh/YUGH/dev/trade/examples/run_data_demo.py)。

## 测试

当前已有基础测试：

- [tests/test_data_models.py](/Users/yugh/YUGH/dev/trade/tests/test_data_models.py)
- [tests/test_storage_paths.py](/Users/yugh/YUGH/dev/trade/tests/test_storage_paths.py)

本地检查：

```bash
conda run -n dev python -m compileall src scripts examples tests
```

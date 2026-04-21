# Data Flow


## 定义
## Market Values
统一 market 取值：
- `cn`
- `hk`
- `us`
A 股额外有 `board`：
- `sh`
- `sz`
- `bj`
港股和美股：
- `board=""`

### 归一化规则
统一列定义：
- `market`
- `board`
- `symbol`
- `provider_symbol`
- `name`
- `cname`
- `open`
- `close`
- `high`
- `low`
- `volume`
- `amount`
- `trade_date`
- `source`

关键规则：

- `cn`
  - `symbol` 去掉前缀，例如 `sz000001 -> 000001`
  - `board` 从代码推断为 `sh/sz/bj`
  - `provider_symbol = board + symbol`，例如 `sh600000`
- `hk`
  - `symbol` 固定补成 5 位，例如 `542 -> 00542`
  - `provider_symbol = symbol`
- `us`
  - `symbol` 用展示代码，例如 `105.MSFT -> MSFT`
  - `provider_symbol` 保留原始请求代码

### 数据源(指定symbol的历史数据)
- `cn`: 新浪历史 JS 接口
- `hk`: `ak.stock_hk_daily(...)`
- `us`: `ak.stock_us_daily(...)`，失败时降级到新浪静态历史接口

- futu.request_trading_days 股票日历(获取当日是否是交易日)
https://openapi.futunn.com/futu-api-doc/quote/request-trading-days.html

### 执行策略(cn/hk/us 流程一致)
每个 `market` 在收盘后进入一个“续跑窗口”，同一条 cron 会被反复触发，但靠缓存、进程互斥和断点续跑避免重复工作。

1. 输入 `market` 和 `trade_date`
2. 抓取当日全市场横截面，保存到 `data_store/day/{market}.{date}.csv`
3. 读取当日 `hist` 和 `progress`，识别已完成 symbol
4. 对当日剩余 symbol 全量抓取最近 2 年历史
5. 每成功一只 symbol，立即追加写入 `data_store/hist/{market}.{date}.csv`
6. 每处理完一只 symbol，立即记录到 `data_store/hist/{market}.{date}.progress.csv`
7. 本轮结束后压实当日 `hist`，再输出合并结果到 `data_store/hist/{market}.merge.{date}.csv`

说明：

- `hist/{market}.{date}.csv` 是当日原始历史结果，保留用于审计和断点续跑
- `hist/{market}.{date}.progress.csv` 记录当日已处理过的 symbol，包括“成功但无历史数据”的情况
- `hist/{market}.merge.{date}.csv` 是按 `market + board + symbol + trade_date` 去重后的当日汇总结果，重复记录以最新为准
- 单票抓取默认带轻量重试；若仍失败，不写入 `progress`，下次 cron 会继续补抓

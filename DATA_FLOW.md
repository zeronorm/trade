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
- `cn`: `ak.stock_zh_a_spot()`
- `hk`: `ak.stock_hk_spot()`
- `us`: `ak.stock_us_spot()`

- futu.request_trading_days 股票日历(获取当日是否是交易日)
https://openapi.futunn.com/futu-api-doc/quote/request-trading-days.html

### 执行策略(cn/hk/us 流程一致)
每个 `market` 只保留一个调度时间点，在该时间点内顺序完成整条流水线，不再拆成两个 cron：

1. 输入 `market` 和 `trade_date`
2. 抓取当日全市场实时行情，归一化后保存到 `day/{market}.{date}.csv`
3. 读取同 market 上一份 `day/*.csv`
4. 对比 `board + symbol`，找出新增 symbol
5. 如果是首次运行，则当前 day 文件里的所有 symbol 都视为新增
6. 为新增 symbol 抓取最近 2 年历史数据
7. 归一化后合并写入 `hist/{market}.csv`

说明：

- `market_day_spot` 与 `symbol_hist` 是同一次 market 同步里的两个阶段
- 加载 `hist/{market}.csv` 即可得到该市场已汇总历史

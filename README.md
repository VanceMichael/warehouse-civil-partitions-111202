# civil-partitions

按门店当地营业日组织带偏移的订单事件，迁移期同时保留旧的 UTC 日期布局。

## 背景与问题

旧实现只按 UTC 日期分区（`warehouse/domain.py` 的 `overview` 仍保留该视图）。
海外门店的同一营业日订单会被 UTC 午夜切到相邻目录；夏令时回拨那晚还存在两个同名
本地小时。单纯重命名目录并不会改变查询语义，因此本项目新增一套**营业日湖表布局**，
并提供从 UTC 布局按分区迁移、对账与回退的完整机制。

## 营业日定义

- 营业日由门店的 **IANA 时区**与**闭店时刻**（整点小时，可超过 24 点）定义。
- 营业日 `D` 覆盖门店本地半开区间 `[D 00:00, D+1 close_hour)`；凌晨闭店前的
  订单归属前一营业日，跨月、跨年由日历对象推进处理。
- 配置按版本追加（`store_configs`），生效时刻由本地日期零点换算成 UTC 固定，
  已写入分区的营业日解释永远绑定当时的 `config_id`，不被后续配置改写。
- 所有跨天运算使用日历对象（`timedelta(days=1)`、`date` 比较），**不按固定
  86400 秒推算**：纽约春跳当天营业日物理跨度 25 小时、回拨当天 27 小时。

## 输入时间规则

- 带 UTC `Z` 或固定偏移（如 `-04:00`）的输入可唯一确定瞬间，归一到 UTC 后接受。
- 无偏移输入按门店时区解释：
  - 落在春季跳时空洞（本地时间不存在）：**拒绝**，不自动顺延；
  - 落在秋季回拨重叠段（同名小时出现两次）：**隔离**到 `_quarantine/`，
    不进入业务分区，可通过 `quarantine` 命令查看；
  - 其余正常时间唯一确定瞬间后接受。

## 物理湖表布局

```
<lake>/
  store=<store>/utc_day=<YYYY-MM-DD>/<sha256>.jsonl          # 旧 UTC 布局
  store=<store>/biz_day=<YYYY-MM-DD>/config=<config_id>/<sha256>.jsonl  # 营业日布局
  _quarantine/<reason_code>/<sha256>.jsonl                   # 隔离区
```

对象按内容 SHA-256 命名、写入 fsync 后硬链接原子发布；每个分区有 `manifest.json`
登记对象清单，读取只打开清单内且 catalog 标记 `active=1` 的对象。

## 查询与扫描证据

查询给出门店与营业日范围后：

1. 把营业日范围按门店时区换算成 UTC 窗口（宁宽勿漏）；
2. 生成保证不漏行的物理分区候选——UTC 布局取窗口覆盖的 UTC 日期，营业日布局
   取每个请求营业日 × 该门店每个配置版本；
3. 只打开存在且 active 的候选分区，不存在的记为 `pruned`（无关分区从未打开）；
4. 用精确时间谓词与营业日归属复核过滤边界行；
5. 同一事件在两种布局都有副本时按权威布局去重，**绝不计两次**；
6. 返回业务行的同时输出 `scan` 说明：候选、实际打开对象、裁剪分区、扫描/保留行数。

## 布局模式与迁移

布局模式记录在 `layout_registry`，并发写入者通过 `BEGIN IMMEDIATE` 串行化，
在同一模式下工作：

- `legacy`：只写 UTC 布局（旧行为，始终可读）；
- `dual`：双写两种布局，读视图以营业日布局为权威、UTC 兜底；
- `business`：只写营业日布局。

迁移按**分区**推进，状态机 `planned -> copied -> verified -> switched`，
每步幂等可重试，失败事务整体回滚不留半态；`rollback` 可把分区权威切回 UTC。
`reconcile` 输出对账摘要（两布局事件集合、内容指纹、权威守恒、迁移状态计数）。

## 环境与验证

Python 3.11 以上，仅用标准库（含 `zoneinfo` 与系统 IANA 时区库），无第三方依赖。

```bash
python3 -m compileall -q warehouse tests scripts
python3 -m unittest discover -s tests -v
python3 scripts/demo.py               # 原 UTC 装载/查询/导出往返
python3 scripts/business_day_demo.py  # 营业日 DST/迁移/对账/扫描证据端到端
```

## 命令一览

```bash
# 登记门店配置（时区、闭店小时、本地生效日期）
python3 -m warehouse config --db w.sqlite3 --store ny --config-id cfg-1 \
    --timezone America/New_York --close-hour 2 --effective 2026-01-01
# 切换布局模式
python3 -m warehouse mode --db w.sqlite3 --set-mode dual
# 分区装载（跳时拒绝、回拨隔离、双写）
python3 -m warehouse ingest --db w.sqlite3 --lake ./lake --input events.json --batch b1
# 营业日查询（含扫描说明）
python3 -m warehouse query --db w.sqlite3 --lake ./lake --store ny \
    --start-day 2026-10-31 --end-day 2026-11-01
# 预览重分区计划 / 迁移单分区 / 回退 / 对账 / 隔离区
python3 -m warehouse plan     --db w.sqlite3 --lake ./lake --store ny
python3 -m warehouse migrate  --db w.sqlite3 --lake ./lake --partition 'store=ny/utc_day=2026-11-01'
python3 -m warehouse rollback --db w.sqlite3 --lake ./lake --partition 'store=ny/utc_day=2026-11-01'
python3 -m warehouse reconcile --db w.sqlite3 --lake ./lake --store ny
python3 -m warehouse quarantine --db w.sqlite3 --lake ./lake
```

关系数据保存在 SQLite，精确数量用十进制文本；导出对象按内容哈希命名，读取会
重新校验。原有 `import` / `inspect` / `export` 命令与 `examples/` 样例保持兼容。

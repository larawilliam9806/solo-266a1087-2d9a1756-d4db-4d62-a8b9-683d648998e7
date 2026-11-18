# Buoy Data Lake Compaction Service

单进程浮标数据湖 API：按 UTC 日分区追加上传 CSV（浮标号、UTC 时刻、盐度），
每次上传落为一个 Parquet 文件；支持按日读取全部记录与当前文件数；支持按日
触发后台压实，把启动时已有的若干小文件合并为一份，降低下游扫描的小文件开销。
另提供**按日快照**：把某一 UTC 日「创建时刻已登记的文件集合」固定下来，之后
凭快照 ID 反复读取这批原始行（保留重复），不受后续补传、压实影响。

技术栈：FastAPI + PyArrow + SQLite（WAL），单进程设计。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

所有状态（Parquet 数据 + SQLite 元数据）都在 `/data` 卷中，容器重启不丢数据。

### 本地运行

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/days/{day}/upload` | 上传 CSV（multipart 文件或 `text/csv` 原始 body），追加为一个 Parquet |
| GET | `/days/{day}/records` | 读取当日全部记录（一致快照），含 `file_count`/`row_count` |
| GET | `/days/{day}/files` | 当日活跃文件清单与文件数 |
| POST | `/days/{day}/snapshots` | 为当日创建快照，返回快照 ID 与创建时的文件/行数 |
| GET | `/days/{day}/snapshots` | 当日仍有效的快照列表 |
| GET | `/snapshots/{snapshot_id}` | 快照内容：创建时的全部原始行（保留重复）及元数据 |
| GET | `/snapshots/{snapshot_id}/records` | 同上（仅取记录体） |
| DELETE | `/snapshots/{snapshot_id}` | 释放快照；该 ID 不再供数（幂等） |
| POST | `/days/{day}/compactions` | 启动后台压实（202）；当日已有压实进行中返回 409 |
| GET | `/days/{day}/compactions` | 当日压实任务列表 |
| GET | `/days/{day}/compactions/{job_id}` | 查询压实任务状态 |
| GET | `/health` | 健康检查 |

`day` 格式为 `YYYY-MM-DD`（UTC 日期分区）。CSV 列为 `buoy_id, ts_utc, salinity`，
表头可选（无表头时按此顺序解析）；时间戳支持 ISO-8601（`Z` 或时区偏移，
裸时间按 UTC 处理）。

### 快照

```bash
# 创建：固定当日「此刻已登记」的文件集合，返回快照 ID
curl -X POST http://localhost:8000/days/2026-11-18/snapshots
# -> {"snapshot_id":"…","day":"2026-11-18","file_count":3,"row_count":120, …}

# 读取：建立时可见的全部原始行（重复行逐条保留），与当前读数集合无关
curl http://localhost:8000/snapshots/<snapshot_id>

# 释放：此后该 ID 的读取返回 404；已开始的读取仍会完整返回
curl -X DELETE http://localhost:8000/snapshots/<snapshot_id>
```

- 原始按日接口 `/days/{day}/records`、`/days/{day}/files` 仍返回**最新**数据。
- 快照按「文件集合」而非「行号区间」固定：创建时取当日 `active` 文件的完整
  清单，因此补传、压实都不会改变快照内容。
- 若你的调用方按日命名空间组织，也可使用 `/days/{day}/snapshots/{snapshot_id}`
  读取、`/days/{day}/snapshots/{snapshot_id}/records` 与对应 `DELETE`；快照 ID
  本身全局唯一，跨日访问返回 404。

### 示例

```bash
# 上传（multipart 或原始 body 均可）
curl -F "file=@obs.csv" http://localhost:8000/days/2026-11-18/upload
curl -X POST -H "Content-Type: text/csv" --data-binary @obs.csv \
     http://localhost:8000/days/2026-11-18/upload

# 读取全部记录与文件数
curl http://localhost:8000/days/2026-11-18/records

# 启动压实并轮询状态
curl -X POST http://localhost:8000/days/2026-11-18/compactions
curl http://localhost:8000/days/2026-11-18/compactions/1

# 快照：创建 → 读取 → 释放
curl -X POST http://localhost:8000/days/2026-11-18/snapshots
curl http://localhost:8000/snapshots/<snapshot_id>
curl -X DELETE http://localhost:8000/snapshots/<snapshot_id>
```

## 一致性与可靠性设计

- **上传原子性**：Parquet 先写 `*.tmp-*` 临时文件，`fsync` 后 `rename` 为正式
  文件并 `fsync` 目录，再在 SQLite 登记为 `active`。读者永远看不到半成品文件；
  崩溃最多留下临时碎片，启动恢复时清理。
- **读取一致性**：读取在同一把锁内完成「快照当日 active 文件列表 + 引用计数
  +1」，因此一次读取必然对应完整的切换前或切换后状态，不会出现新旧混杂；
  响应中的 `file_count` 与实际读取的文件集合严格一致。
- **压实切换原子性**：合并输出完整落盘（tmp+fsync+rename）后，「退役输入文件
  + 登记输出文件 + 任务置为 done」在**单个 SQLite 事务**中提交。提交前崩溃
  → 原文件全部仍在，任务重启后续跑；提交后崩溃 → 切换已生效。不存在中间态。
- **压实期间可写可读**：合并只处理任务启动时快照的输入集合；期间追加的文件
  不在集合内，切换时原样保留、全程可读，不会被遗漏或误删。上传、读取均不
  等待压实结束。
- **同日只受理一项压实**：SQLite 部分唯一索引
  `WHERE status='running'` 在数据库层面保证，并发请求第二个返回 409，
  进程重启后约束依然有效。
- **失败保留原数据**：合并或落盘失败 → 任务标记 `failed`（带错误信息），
  输入文件保持 `active`，可修复后重新发起压实。
- **中断恢复**：启动时 `recover()` 先清理退役文件、孤儿 Parquet（未登记的
  上传残留、被取代的压实输出）和 tmp 碎片，再把仍处于 `running` 的任务重新
  投入执行（其输入必然仍全部 active，重跑是安全的）。已提交的上传不受影响。
  仍被有效快照引用（pinned）的文件不会被这一步删除，因此重启不破坏旧快照。
- **旧文件回收**：退役文件仅在在途读取引用计数归零后才 unlink；回收只针对
  登记表中 `retired` 的文件，`active` 文件（包括压实期间新追加的）永远不会
  被误删。另有后台周期清扫兜底。
- **快照创建原子性**：快照行与其固定文件集合在**单个 SQLite 事务**中写入
  （`BEGIN IMMEDIATE`），并与上传/压实切换的登记事务串行。因此一次创建必然
  对应一组完整、已登记的文件；创建失败或进程中断只会留下未提交事务，不会
  暴露半份快照，也不影响已成功的上传或压实。
- **快照隔离**：创建后上传的新文件、以及压实产生的新文件都不属于旧快照；
  快照读取按其固定清单逐文件读取，因此后续补传与压实都不改变旧快照内容。
- **压实/重启不破坏旧快照**：回收器与启动清理都会跳过仍被有效快照引用的
  文件；即使这些文件已被压实退役，字节也会保留到快照被释放。快照与固定
  集合持久化在 SQLite 中，容器/进程重启后依旧可读。
- **释放语义**：释放在一个事务内同时写入 `released_at` 墓碑并删除固定清单。
  之后该 ID 的读取/查询返回 404，且不可恢复；已在进行的读取持有独立的
  引用计数，仍会完整读完（不会读到一半丢文件）。引用计数归零后，只为该
  快照保留的字节在下一次清扫时回收。
- **空日快照**：当日尚无文件时也允许创建快照（空集合），之后的上传不会
  进入该快照。

## 目录布局

```
data/
  lake_meta.db                 # SQLite 元数据（files / compactions / snapshots / snapshot_files）
  dt=2026-11-18/
    part-<uuid>.parquet        # 上传产生的文件
    compacted-<job>-<id>.parquet  # 压实输出
```

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `./data`（容器内 `/data`） | 数据与元数据目录 |
| `LAKE_GC_INTERVAL_SEC` | `2` | 退役文件周期清扫间隔 |
| `LAKE_COMPACTION_DELAY_SEC` | `0` | 测试/演示钩子：合并前人为延迟，便于观察压实期间的并发读写 |

> 注意：一致性依赖进程内锁 + SQLite 事务，请以单进程运行（不要多 worker
> 水平扩展）。

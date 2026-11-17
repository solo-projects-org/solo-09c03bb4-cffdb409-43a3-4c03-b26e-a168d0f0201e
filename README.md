# 样品入仓链路诊断平台

检验数仓工程师用于查清"样品迟迟未入仓"原因的诊断工具：到底是**等名额**、**源站慢**还是**质控拖延**。
提交样品号清单后真实执行 **取数 → 并行两项质控 → 入仓** 全流程，跨服务记录每一次调用的
真实起止、等待、关联与重试归属，按批次展开时间线并给出有据可查的完成结论。

## 架构（Docker Compose 三服务）

| 服务 | 目录 | 端口 | 职责 |
|---|---|---|---|
| orchestrator | `app/` | 8000 | 诊断网页 + 流程编排 + SQLite 记录（批次/跨度/入仓） |
| source | `source_app/` | 8001 | 独立检验源：返回样品号+浓度；名额槽位、延迟、首次失败可调 |
| qc | `qc_app/` | 8002 | 质控服务：①浓度非负 ②样品号对应，两项独立调延迟/首次失败 |

## 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

无 Docker 的本地开发（Python 3.11+，pip 安装 fastapi/uvicorn/httpx）：

```bash
cd source_app && ORCH_URL=http://localhost:8000 uvicorn main:app --port 8001 &
cd qc_app     && ORCH_URL=http://localhost:8000 uvicorn main:app --port 8002 &
cd app        && SOURCE_URL=http://localhost:8001 QC_URL=http://localhost:8002 \
                 uvicorn main:app --port 8000 &
```

## 使用

1. **① 运行参数**：实时调整源站延迟/并行名额/首次失败、两项质控各自的延迟/首次失败、
   取数超时与最大尝试次数，"应用配置"即下发到源站与质控服务。
2. **② 提交样品号清单**：每行一个或逗号分隔，真实跑完整流程。
   演示钩子：样品号含 `NEG` → 源站返回负浓度（质控①不通过）；含 `MIX` → 返回错位样品号（质控②不通过）。
3. **③ 批次列表**：状态/入仓数/墙钟/瓶颈判定，自动刷新；多批交错运行记录互不串。
4. **批次详情**：完成结论（以实际调用与入仓记录为据）、决定完成时间的等待与执行链、
   分阶段统计、跨服务时间线（黄框=关键链）、调用明细、入仓记录；可一键**重跑**对比瓶颈变化。

## 关键语义

- **取数超时重试**：客户端超过 `client_timeout_ms` 放弃等待并发起新尝试；旧请求**不被取消**，
  源站继续执行（服务端跨度照常上报）。首个有效结果胜出（effective），其余完成时记**迟到（late）**。
- **迟到不重复入仓**：入仓表 `UNIQUE(batch_id, sample_id)` + `INSERT OR IGNORE`，
  且只有有效尝试的结果会进入入仓流程。
- **并行质控不相加**：两项质控并行，墙钟取较慢者；重叠重试、多样品并行同样不按求和计总耗时，
  页面同时给出"耗时求和"与"墙钟跨度"两列以示区分。
- **等名额**：源站槽位（`source_slots`）占满后请求排队，排队时长由源站服务端以 `queue_ms` 上报。
- **失败批次**：不生成成功链，只列出阻断样品、阻断环节与错误明细。
- **结论有据**：完成结论只依据 `spans`（真实调用）与 `warehouse`（真实入仓记录）实时计算。

## 自测

三服务启动后执行（27 项断言：基线、慢源超时/迟到/不重入仓、等名额、瓶颈迁移、
首次失败、质控阻断、NEG/MIX 钩子、多批交错不串记录）：

```bash
python scripts/selftest.py http://localhost:8000
```

## API 摘要

- `POST /api/batches` `{sample_ids:[...]}` 提交批次；`GET /api/batches` 列表；`GET /api/batches/{id}` 详情
- `GET/POST /api/config` 运行参数（POST 会下发到源站/质控）
- `POST /api/ingest` 源站/质控上报服务端跨度（按 `request_id` 与客户端跨度关联）
- 源站：`GET /fetch`、`GET/POST /config`；质控：`POST /check/{nonneg|match}`、`GET/POST /config`

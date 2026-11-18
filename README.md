# 固件构建流水线 API

FastAPI + SQLite 实现的 DAG 构建流水线：接收至多 12 步的 JSON DAG，在挂载工作目录中
执行真实 shell 命令，全局最多 2 步并行，带依赖调度、取消、超时、进程组清理和重启恢复。

## 功能与语义对照

| 需求 | 实现 |
| --- | --- |
| 至多 12 步 JSON DAG，含命令/依赖/超时 | `POST /builds`，Pydantic 校验 + 重复 id / 悬空依赖 / 自依赖 / 有环检测（400/422） |
| 挂载工作目录执行真实命令 | 每构建独立目录 `$PIPELINE_DATA_DIR/workdir/<build_id>/`，compose 挂载命名卷 `/data` |
| 全局最多两步并行 | 单 worker 内 `asyncio.Semaphore(2)`，名额覆盖步骤全生命周期 |
| 依赖全成功才启动 | 调度器按 DAG 推进，依赖未全部 `succeeded` 不启动 |
| 失败使后代跳过，独立分支继续 | 上游出现失败/取消/中断即置后代 `skipped`；无依赖关系的分支不受影响 |
| 整次取消停止所有未完成步骤 | `POST /builds/{id}/cancel`，per-build 事件即时通知；未启动步骤不再启动 |
| 超时/取消终止对应进程组 | 每步 `start_new_session=True` 独立进程组，`killpg(PGID, SIGTERM→SIGKILL)` |
| 主命令退出后慢子进程清理并判失败 | 退出后扫描 `/proc` 检查组内残留，有残留即清理且步骤判 `failed` |
| 进程组清空前不释放名额/不确认终态 | 信号量在清理完成、终态落库后才释放；期间状态可见为 `cleaning_up` |
| 慢清理不阻塞另一名额 | 清理是异步等待，只占自己的名额，另一名额正常调度其他步骤/构建 |
| 重启先清理旧残留再标中断 | startup 恢复：PID+starttime 双重身份确认后终止旧组，再统一置 `interrupted` |
| 未跑步骤不再启动、不误杀后来构建 | 恢复不恢复调度；身份不符（PID 复用）的进程绝不触碰 |
| 重复取消不改终态 | 终态构建上的取消请求幂等忽略，返回说明，不写库 |
| 逐步状态与日志 | `GET /builds/{id}` 全量状态；`GET /builds/{id}/steps/{sid}/logs`（支持 `?tail=N`） |

步骤状态机：`pending → running → cleaning_up → succeeded|failed|cancelled|interrupted`，
失败后代为 `skipped`。构建终态：全部成功 `succeeded`；有失败 `failed`；
取消导致的终止 `cancelled`；重启中断 `interrupted`。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# API:      http://localhost:8000
# Swagger:  http://localhost:8000/docs
# 健康检查: http://localhost:8000/health
```

容器以 `tini` 作为 PID 1 回收孤儿/僵尸进程；数据全部落在命名卷 `pipeline-data`
（SQLite、工作目录、步骤日志），重启不丢、恢复逻辑依赖它。

## 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
PIPELINE_DATA_DIR=./data .venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 示例构建

`examples/` 下提供三类（另含超时示例）：

- `success_build.json` — prepare → compile → unit-test → package 全成功链路
- `failure_build.json` — 编译失败、后代跳过、独立分支继续
- `slow_child_build.json` — 主命令退出 0 但派生 `sleep 300` 慢子进程，验证清理与判失败
- `timeout_build.json` — 2s 超时终止睡眠 60s 的进程组

一键演示（API 已启动时）：

```bash
.venv/bin/python examples/run_demo.py            # 默认 http://127.0.0.1:8000
```

手动提交：

```bash
curl -s -X POST localhost:8000/builds \
  -H 'Content-Type: application/json' \
  --data @examples/success_build.json
curl -s localhost:8000/builds/1
curl -s localhost:8000/builds/1/steps/compile/logs
curl -s -X POST localhost:8000/builds/1/cancel
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/builds` | 提交 DAG 构建（201 返回含逐步状态） |
| GET | `/builds?limit=N` | 构建列表（新→旧） |
| GET | `/builds/{id}` | 构建与全部步骤状态 |
| POST | `/builds/{id}/cancel` | 整次取消（幂等） |
| GET | `/builds/{id}/steps/{step_id}/logs?tail=N` | 步骤日志 |
| GET | `/health` | 健康检查与最近一次启动恢复报告 |
| GET | `/docs` | Swagger UI |

## 进程组与重启恢复的关键设计

1. 每个步骤由 `/bin/sh -c <command>` 在新会话中启动，主命令为进程组组长；
   命令派生出的所有子进程默认同组，可被 `killpg` 一次性覆盖。
2. 步骤终态确认前必须满足：`/proc` 扫描该 PGID 已无任何非僵尸成员。
   清理序列为 SIGTERM 宽限（默认 5s）→ SIGKILL → 硬等排空（默认 60s 有界）。
3. 主命令退出码为 0 但组内仍有成员时，清理后步骤判 `failed`，杜绝"误发包"。
4. SQLite 中记录每步的 `pid / pgid / proc_starttime`。重启恢复时先校验
   `/proc/<pid>/stat` 的 starttime 与 pgid 均匹配才终止；PID 已被复用的
   后来构建进程不会被误杀。组长已死但组内仍有成员时，因 PGID 不可能被复用，
   残留成员仍会被清理。
5. 恢复在应用开始接受请求前完成；旧构建统一置 `interrupted`，调度不恢复。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_validation.py`：DAG 校验、12 步上限、超时正数、进程身份识别。
- `tests/test_integration.py`：以真实 uvicorn 子进程端到端验证成功、失败跳过、
  慢子进程清理判失败、超时、并行上限 2、整次取消、重复取消幂等、慢清理不占
  另一名额、重启恢复（含不误杀后来构建）、错误请求与文档接口。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PIPELINE_DATA_DIR` | `./data` | SQLite/工作目录/日志根目录 |
| `PIPELINE_MAX_STEPS` | `12` | 单构建步骤上限 |
| `PIPELINE_MAX_PARALLEL` | `2` | 全局并行名额 |
| `PIPELINE_TIMEOUT_CAP_SECONDS` | `86400` | 单步超时上限 |
| `PIPELINE_TERM_GRACE_SECONDS` | `5` | SIGTERM 宽限 |
| `PIPELINE_KILL_GRACE_SECONDS` | `60` | SIGKILL 后硬等上限 |

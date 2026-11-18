"""请求/响应模型与 DAG 校验。"""
from __future__ import annotations

from typing import Annotated, Optional

from pydantic import BaseModel, Field, field_validator

from . import config

# 步骤与构建的状态机
# pending 未启动 / running 运行中 / cleaning_up 主进程已退出，正在清空进程组
# succeeded / failed / skipped / cancelled / interrupted 均为终态
STEP_TERMINAL = {"succeeded", "failed", "skipped", "cancelled", "interrupted"}
BUILD_TERMINAL = {"succeeded", "failed", "cancelled", "interrupted"}

# 取消/超时后清理时采用的终态
CANCEL_TERMINAL = "cancelled"
TIMEOUT_TERMINAL = "failed"


class StepIn(BaseModel):
    id: Annotated[str, Field(min_length=1, max_length=128)]
    command: Annotated[str, Field(min_length=1, max_length=8192)]
    depends_on: list[str] = Field(default_factory=list)
    timeout_seconds: Annotated[float, Field(gt=0, le=config.TIMEOUT_CAP_SECONDS)]

    @field_validator("id", "command")
    @classmethod
    def _no_surrounding_whitespace(cls, v: str) -> str:
        if v != v.strip():
            raise ValueError("字符串首尾不允许空白字符")
        return v

    @field_validator("depends_on")
    @classmethod
    def _unique_deps(cls, v: list[str]) -> list[str]:
        if len(v) != len(set(v)):
            raise ValueError("依赖列表存在重复")
        return v


class BuildCreate(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=128)]
    steps: Annotated[list[StepIn], Field(min_length=1, max_length=config.MAX_STEPS)]


class StepOut(BaseModel):
    id: str
    command: str
    depends_on: list[str]
    timeout_seconds: float
    status: str
    exit_code: Optional[int]
    detail: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]


class BuildOut(BaseModel):
    id: int
    name: str
    status: str
    cancel_requested: bool
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    detail: Optional[str]
    steps: list[StepOut]


class LogOut(BaseModel):
    build_id: int
    step_id: str
    status: str
    truncated: bool
    content: str


class CancelOut(BaseModel):
    id: int
    status: str
    cancel_requested: bool
    detail: str


def validate_dag(steps: list[StepIn]) -> None:
    """结构之外的 DAG 语义校验：重复 id、悬空依赖、自依赖、有环。"""
    ids = [s.id for s in steps]
    duplicates = {i for i in ids if ids.count(i) > 1}
    if duplicates:
        raise ValueError(f"步骤 id 重复: {sorted(duplicates)}")

    id_set = set(ids)
    for step in steps:
        for dep in step.depends_on:
            if dep == step.id:
                raise ValueError(f"步骤 {step.id} 不能依赖自身")
            if dep not in id_set:
                raise ValueError(f"步骤 {step.id} 依赖了不存在的步骤 {dep}")

    # Kahn 拓扑排序检测有向环
    indegree = {s.id: len(set(s.depends_on)) for s in steps}
    adjacency: dict[str, list[str]] = {s.id: [] for s in steps}
    for step in steps:
        for dep in step.depends_on:
            adjacency[dep].append(step.id)

    ready = [sid for sid, deg in indegree.items() if deg == 0]
    visited = 0
    while ready:
        current = ready.pop()
        visited += 1
        for nxt in adjacency[current]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                ready.append(nxt)
    if visited != len(steps):
        raise ValueError("DAG 存在循环依赖")

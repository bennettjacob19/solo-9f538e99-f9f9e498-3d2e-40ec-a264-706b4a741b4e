"""DAG 校验与进程身份识别的纯单元测试。"""
from __future__ import annotations

import os

import pytest
from pydantic import ValidationError

from app.models import BuildCreate, validate_dag
from app.proc import is_same_process, read_starttime


def _step(uid: str, deps=None, timeout: float = 10.0, command: str = "true"):
    return {
        "id": uid,
        "command": command,
        "depends_on": deps or [],
        "timeout_seconds": timeout,
    }


def test_valid_diamond_dag():
    payload = BuildCreate(
        name="n",
        steps=[
            _step("a"),
            _step("b", ["a"]),
            _step("c", ["a"]),
            _step("d", ["b", "c"]),
        ],
    )
    validate_dag(payload.steps)  # 不抛异常即通过


def test_cycle_detected():
    payload = BuildCreate(
        name="n",
        steps=[_step("a", ["b"]), _step("b", ["a"])],
    )
    with pytest.raises(ValueError, match="循环"):
        validate_dag(payload.steps)


def test_missing_dependency():
    payload = BuildCreate(name="n", steps=[_step("a", ["ghost"])])
    with pytest.raises(ValueError, match="不存在"):
        validate_dag(payload.steps)


def test_self_dependency():
    payload = BuildCreate(name="n", steps=[_step("a", ["a"])])
    with pytest.raises(ValueError, match="自身"):
        validate_dag(payload.steps)


def test_duplicate_step_ids():
    payload = BuildCreate(name="n", steps=[_step("a"), _step("a")])
    with pytest.raises(ValueError, match="重复"):
        validate_dag(payload.steps)


def test_step_count_limit(monkeypatch):
    from app import models

    monkeypatch.setattr(models.config, "MAX_STEPS", 12, raising=False)
    with pytest.raises(ValidationError):
        BuildCreate(name="n", steps=[_step(f"s{i}") for i in range(13)])
    ok = BuildCreate(name="n", steps=[_step(f"s{i}") for i in range(12)])
    assert len(ok.steps) == 12


def test_timeout_must_be_positive():
    with pytest.raises(ValidationError):
        BuildCreate(name="n", steps=[_step("a", timeout=0)])


def test_starttime_identity_guard():
    my_pid = os.getpid()
    start = read_starttime(my_pid)
    assert start is not None
    # 同 pid 同 starttime -> 是同一进程
    assert is_same_process(my_pid, start) is True
    # starttime 被篡改 -> 判为不同进程（重启后 PID 复用场景）
    assert is_same_process(my_pid, start + 999_999) is False
    # 不存在的 pid
    assert is_same_process(2_000_000, 123) is False
    # starttime 缺失时保守拒绝
    assert is_same_process(my_pid, None) is False

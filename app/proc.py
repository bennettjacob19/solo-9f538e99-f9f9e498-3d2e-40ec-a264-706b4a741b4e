"""进程组管控：以独立 PGID 启动、超时/取消整组终止、慢子进程清理与识别。

设计要点：
- 每个步骤用 ``start_new_session=True`` 启动，主命令成为新会话/新进程组组长，
  其派生出的子进程默认留在同一进程组，可整组 ``killpg``。
- 进程身份用 (pid, /proc starttime) 双重确认，防止 API 重启后 PID 被复用
  而误杀后来构建的进程。
- 清理顺序：SIGTERM 宽限 -> 轮询排空 -> SIGKILL -> 硬等排空；硬等有界。
- 只要进程组里还有非僵尸成员（无论是否是直接子进程），就不视为清空，
  名额不释放、步骤终态不确认。
"""
from __future__ import annotations

import errno
import os
import signal
from pathlib import Path
from typing import Optional

from . import config

_PROC = Path("/proc")


def read_starttime(pid: int) -> Optional[int]:
    """读取 /proc/<pid>/stat 的第 22 字段（进程启动时钟节拍）。"""
    try:
        data = (_PROC / str(pid) / "stat").read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    # comm 中可能含空格/括号，按最后一个 ')' 切分
    _, _, rest = data.partition(")")
    fields = rest.split()
    # ')' 之后第 20 个字段即 starttime（整体第 22 个字段）
    if len(fields) < 20:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None


def is_same_process(pid: int, starttime: Optional[float]) -> bool:
    """PID 与启动时间双重校验，starttime 缺失时保守地认为不是同一进程。"""
    if starttime is None:
        return False
    current = read_starttime(pid)
    if current is None:
        return False
    return current == int(starttime)


def list_pgid_members(pgid: int) -> list[tuple[int, str]]:
    """列出进程组内仍存活的非僵尸成员 (pid, state)。"""
    members: list[tuple[int, str]] = []
    for entry in _PROC.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            stat = (entry / "stat").read_text()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        _, _, rest = stat.partition(")")
        fields = rest.split()
        if len(fields) < 6:
            continue
        state = fields[0]
        try:
            proc_pgid = int(fields[3])
        except ValueError:
            continue
        if proc_pgid == pgid and state != "Z":
            members.append((pid, state))
    return members


def _kill_group(pgid: int, sig: int) -> None:
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass
    except OSError as exc:  # pragma: no cover - 极端权限场景
        if exc.errno != errno.ESRCH:
            raise


async def terminate_process_group(
    proc: Optional["object"],
    pgid: int,
    reason: str,
    log_lines: list[str],
    *,
    term_grace: Optional[float] = None,
    kill_grace: Optional[float] = None,
    poll_interval: Optional[float] = None,
) -> bool:
    """排空整个进程组。返回 True 表示已清空，False 表示超过硬等上限仍有残留。

    无论调用方是超时、取消还是正常退出后的兜底清理，都走同一条路径，
    保证"组里还有成员就不返回成功"。
    """
    import asyncio

    term_grace = config.TERM_GRACE_SECONDS if term_grace is None else term_grace
    kill_grace = config.KILL_GRACE_SECONDS if kill_grace is None else kill_grace
    poll_interval = config.PROC_POLL_INTERVAL if poll_interval is None else poll_interval

    # 先确认是否真的有残留（正常退出且无慢子进程时直接回收直接子进程即可）
    await asyncio.sleep(0)
    leftovers = await asyncio.to_thread(list_pgid_members, pgid)
    if not leftovers:
        if proc is not None and proc.returncode is None:  # type: ignore[attr-defined]
            await proc.wait()  # type: ignore[attr-defined]
        return True

    log_lines.append(
        f"[runner] {reason}: 进程组 {pgid} 仍有 {len(leftovers)} 个成员，"
        f"发送 SIGTERM: {leftovers}"
    )
    await asyncio.to_thread(_kill_group, pgid, signal.SIGTERM)

    term_deadline = asyncio.get_running_loop().time() + term_grace
    while asyncio.get_running_loop().time() < term_deadline:
        await asyncio.sleep(poll_interval)
        leftovers = await asyncio.to_thread(list_pgid_members, pgid)
        if not leftovers:
            break
    else:
        log_lines.append(
            f"[runner] SIGTERM 宽限 {term_grace}s 后仍有残留，发送 SIGKILL: {leftovers}"
        )
        await asyncio.to_thread(_kill_group, pgid, signal.SIGKILL)
        kill_deadline = asyncio.get_running_loop().time() + kill_grace
        while asyncio.get_running_loop().time() < kill_deadline:
            await asyncio.sleep(poll_interval)
            leftovers = await asyncio.to_thread(list_pgid_members, pgid)
            if not leftovers:
                break
        else:
            # 有界等待的安全网：理论上 init 回收后这里不会到
            log_lines.append(
                f"[runner] 错误: 进程组 {pgid} 在硬等 {kill_grace}s 后仍有残留: {leftovers}"
            )
            return False

    # 组已排空：回收直接子进程（可能已被 init 回收，ProcessLookupError 可忽略）
    if proc is not None and proc.returncode is None:  # type: ignore[attr-defined]
        try:
            await proc.wait()  # type: ignore[attr-defined]
        except ProcessLookupError:
            pass
    log_lines.append(f"[runner] 进程组 {pgid} 已清空")
    return True


async def spawn(
    command: str, workdir: Path, env: dict[str, str], stdout, stderr
) -> tuple["object", int, int, int]:
    """以独立会话/进程组启动命令，返回 (proc, pid, pgid, starttime)。"""
    import asyncio

    proc = await asyncio.create_subprocess_exec(
        "/bin/sh",
        "-c",
        command,
        cwd=workdir,
        env=env,
        stdout=stdout,
        stderr=stderr,
        start_new_session=True,
    )
    pid = proc.pid
    # 新会话中组长 pid == pgid。极短命令可能在 getpgid 前就退出（ESRCH），
    # 此时进程组要么已空、要么只剩同组孤儿，后续 list_pgid_members 仍能识别，
    # 因此这里直接按 pid 作为 pgid 继续，不把它误判为启动失败。
    try:
        pgid = os.getpgid(pid)
    except ProcessLookupError:
        pgid = pid
    starttime = read_starttime(pid)
    if starttime is None:
        # 进程已退出属于正常竞态；starttime 仅用于重启身份校验，置 0 即可，
        # 恢复时 is_same_process(pid, 0) 不会命中任何真实进程。
        starttime = 0
    return proc, pid, pgid, starttime

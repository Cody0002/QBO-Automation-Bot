from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _read_lock_pid(lock_path: Path) -> int | None:
    try:
        text = lock_path.read_text(encoding="utf-8", errors="ignore").strip()
    except Exception:
        return None
    for line in text.splitlines():
        if line.startswith("pid="):
            try:
                return int(line.split("=", 1)[1].strip())
            except Exception:
                return None
    return None


def _try_create_lock(lock_path: Path) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = f"pid={os.getpid()}\nstarted_at={int(time.time())}\n"
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    try:
        os.write(fd, payload.encode("utf-8"))
    finally:
        os.close(fd)
    return True


@contextmanager
def single_instance_lock(lock_name: str):
    """
    Prevent concurrent runs of the same stage across processes.
    Returns True when lock acquired, False otherwise.
    """
    lock_path = Path(".locks") / f"{lock_name}.lock"
    acquired = _try_create_lock(lock_path)

    # Handle stale lock (owner process no longer alive).
    if not acquired:
        owner_pid = _read_lock_pid(lock_path)
        if owner_pid is not None and not _pid_exists(owner_pid):
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass
            acquired = _try_create_lock(lock_path)

    try:
        yield acquired
    finally:
        if acquired:
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass


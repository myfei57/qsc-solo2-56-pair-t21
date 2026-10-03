"""门控队列的并发锁。

一把锁覆盖两层并发：

* 进程内多个线程同时入队/泵送：用可重入 :class:`threading.RLock` 串行化；
* 多个控制服务进程共用同一存储目录：用 ``fcntl.flock`` 排他锁跨进程互斥。

锁按列车隔离（每列车一条执行线），不同列车的入队与执行互不阻塞。锁文件只在
``pq/locks/<train>.lock`` 上持有，不参与状态语义，崩溃后由内核自动释放。
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import threading
from types import TracebackType

from ...store.jsonfile import JsonStore
from .ledger import _safe


class TrainLock:
    """单列车的进程内 + 跨进程排他锁。"""

    def __init__(self, store: JsonStore, train_id: str) -> None:
        directory = os.path.join(store.root, "pq", "locks")
        os.makedirs(directory, exist_ok=True)
        self._path = os.path.join(directory, f"{_safe(train_id)}.lock")
        self._rlock = threading.RLock()
        self._handle: object | None = None

    def acquire(self) -> "TrainLock":
        self._rlock.acquire()
        try:
            if self._handle is None:
                self._handle = open(self._path, "a+", encoding="utf-8")
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        except OSError:
            self._rlock.release()
            raise
        return self

    def release(self) -> None:
        handle = self._handle
        if handle is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
            self._handle = None
        self._rlock.release()

    def __enter__(self) -> "TrainLock":
        return self.acquire()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()


class LockRegistry:
    """按列车复用锁对象（RLock 必须在同进程内共享才有效）。"""

    def __init__(self, store: JsonStore) -> None:
        self._store = store
        self._guard = threading.Lock()
        self._locks: dict[str, TrainLock] = {}

    def get(self, train_id: str) -> TrainLock:
        with self._guard:
            lock = self._locks.get(train_id)
            if lock is None:
                lock = TrainLock(self._store, train_id)
                self._locks[train_id] = lock
            return lock

    @contextlib.contextmanager
    def hold(self, train_id: str):
        lock = self.get(train_id)
        lock.acquire()
        try:
            yield lock
        finally:
            lock.release()

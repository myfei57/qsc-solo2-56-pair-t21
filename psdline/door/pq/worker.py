"""后台泵送线程。

确定性仿真与测试直接调用 :meth:`PriorityQueueRunner.pump`；真实在线服务可以用
:class:`PQWorker` 起一个守护线程，周期性地把本进程接触过的全部列车向前泵送。
紧急入队通过条件变量立即唤醒，不必等下一个轮询间隔——紧急关门的实时性由
:meth:`PriorityQueueRunner.submit` 的抢占语义保证，worker 只负责"尽快执行到它"。
"""

from __future__ import annotations

import threading

from .runner import PriorityQueueRunner


class PQWorker:
    """单线程轮询所有已知列车的后台泵送器。"""

    def __init__(self, runner: PriorityQueueRunner, interval_s: float = 0.05) -> None:
        self._runner = runner
        self._interval = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> "PQWorker":
        with self._lock:
            if self.running:
                return self
            self._stop.clear()
            thread = threading.Thread(target=self._loop, name="pq-worker", daemon=True)
            self._thread = thread
            thread.start()
        return self

    def stop(self, timeout: float | None = 1.0) -> None:
        self._stop.set()
        # 唤醒可能正在等待的循环，让它尽快观察到退出事件。
        self._runner.idle_wait(0)
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
        self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            advanced = False
            for train_id in self._runner.touched_trains():
                if self._stop.is_set():
                    return
                try:
                    result = self._runner.pump(train_id)
                except Exception:  # pragma: no cover - 真实线程内失败不能拖垮进程
                    continue
                if result.get("ran_unit") or result.get("claimed"):
                    advanced = True
            if not advanced:
                # 空闲时等待新入队唤醒或轮询间隔，避免空转。
                self._runner.idle_wait(self._interval)

"""哈希链审计流水（优先级门控队列的唯一事实源）。

流水落在存储目录下的 ``pq/events/<train>.jsonl``，每行一个 JSON 事件，只追加不
重写。每条事件携带前一条事件的 SHA-256（``prev_hash``），首条以固定创世锚点为
前驱，因此任何对历史的增、删、改都会在 :func:`verify_chain` 的断链处被发现。

写入顺序是"先追加并 ``fsync`` 流水，再让上层原子替换状态快照"。崩溃在两者之间
时，丢掉快照从流水重放即可；崩溃在追加中途留下的半截尾行会被
:func:`recover_tail` 截掉（该事件的物理动作尚未发生或会被恢复流程幂等重放）。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import Iterable

from ...errors import StorageError
from ...store.jsonfile import JsonStore
from .model import LedgerEvent

GENESIS = "0" * 64


def canonical(event_seq: int, at: float, event_type: str, train_id: str,
              command_id: str, payload: dict, prev_hash: str) -> bytes:
    """生成事件的规范字节串（字段顺序固定，保证跨进程哈希一致）。"""

    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    line = f"{event_seq}\n{at!r}\n{event_type}\n{train_id}\n{command_id}\n{body}\n{prev_hash}"
    return line.encode("utf-8")


def digest(event_seq: int, at: float, event_type: str, train_id: str,
           command_id: str, payload: dict, prev_hash: str) -> str:
    return hashlib.sha256(
        canonical(event_seq, at, event_type, train_id, command_id, payload, prev_hash)
    ).hexdigest()


def event_path(store: JsonStore, train_id: str) -> str:
    """返回某列车流水的绝对路径（与快照共用存储根，名字做安全转义）。"""

    return os.path.join(store.root, "pq", "events", f"{_safe(train_id)}.jsonl")


def _safe(train_id: str) -> str:
    encoded = train_id.encode("utf-8").hex()
    return f"tr-{encoded}"


def _unsafe(name: str) -> str:
    if not name.startswith("tr-") or not name.endswith(".jsonl"):
        return ""
    try:
        return bytes.fromhex(name[3:-6]).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return ""


class HashLedger:
    """单列车的追加式哈希链流水。"""

    def __init__(self, store: JsonStore, train_id: str) -> None:
        self._store = store
        self._train_id = train_id
        self._path = event_path(store, train_id)
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        events, _truncated = self._read_all()
        self._events = events
        self._seq = len(events)
        self._tail = events[-1].hash if events else GENESIS

    @property
    def train_id(self) -> str:
        return self._train_id

    @property
    def path(self) -> str:
        return self._path

    def head_seq(self) -> int:
        """返回已落盘事件条数。"""

        with self._lock:
            return self._seq

    def append(self, event_type: str, command_id: str, at: float,
               payload: dict | None = None) -> LedgerEvent:
        """追加一条事件，``fsync`` 后返回；前驱哈希由链尾自动接续。"""

        body = dict(payload or {})
        with self._lock:
            seq = self._seq
            prev = self._tail
            h = digest(seq, at, event_type, self._train_id, command_id, body, prev)
            event = LedgerEvent(
                seq=seq,
                at=at,
                type=event_type,
                train_id=self._train_id,
                command_id=command_id,
                payload=body,
                prev_hash=prev,
                hash=h,
            )
            try:
                with open(self._path, "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(event.as_dict(), ensure_ascii=False) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as error:
                raise StorageError(f"追加门控流水失败: {error}", component="pq") from error
            self._events.append(event)
            self._seq += 1
            self._tail = h
            return event

    def events(self) -> list[LedgerEvent]:
        """返回本列车的全部事件（内存视图，调用方不得修改）。"""

        with self._lock:
            return list(self._events)

    def verify(self) -> int:
        """从磁盘重新读取并校验全链，返回校验通过的事件条数。"""

        events, truncated = self._read_all()
        expected_prev = GENESIS
        for event in events:
            if event.prev_hash != expected_prev:
                raise StorageError(
                    f"门控流水在第 {event.seq} 条断链（前驱不符）", component="pq"
                )
            h = digest(
                event.seq, event.at, event.type, event.train_id,
                event.command_id, event.payload, event.prev_hash,
            )
            if h != event.hash:
                raise StorageError(
                    f"门控流水在第 {event.seq} 条哈希不符（历史可能被篡改）",
                    component="pq",
                )
            expected_prev = event.hash
        if truncated:
            raise StorageError("门控流水末尾存在被截断的半条事件", component="pq")
        return len(events)

    def resync(self, truncate_torn: bool = True) -> int:
        """从磁盘重新加载整条链，刷新内存链尾，返回新增（追上）的事件条数。

        跨进程共用同一流水时，本进程内存里的链尾可能落后于磁盘。所有追加都在
        ``flock`` 排他锁内进行，因此在持锁打开流程里先 :meth:`resync` 到磁盘最新，
        再计算下一条的 ``prev_hash``，就能保证多进程序号连续、链不断。
        ``truncate_torn`` 为真时顺带截掉末尾写了一半的事件行。
        """

        with self._lock:
            removed = 0
            events, truncated = self._read_all()
            if truncate_torn and truncated and os.path.isfile(self._path):
                good_lines: list[str] = []
                with open(self._path, "r", encoding="utf-8") as handle:
                    for raw in handle:
                        text = raw.rstrip("\n")
                        if not text:
                            continue
                        try:
                            json.loads(text)
                        except json.JSONDecodeError:
                            removed += len(raw.encode("utf-8"))
                            continue
                        good_lines.append(text)
                if removed:
                    with open(self._path, "w", encoding="utf-8") as handle:
                        for line in good_lines:
                            handle.write(line + "\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                events, _ = self._read_all()
            added = max(0, len(events) - len(self._events))
            self._events = events
            self._seq = len(events)
            self._tail = events[-1].hash if events else GENESIS
            self._torn_removed = removed
            return added

    def _read_all(self) -> tuple[list[LedgerEvent], bool]:
        if not os.path.isfile(self._path):
            return [], False
        events: list[LedgerEvent] = []
        truncated = False
        with open(self._path, "r", encoding="utf-8") as handle:
            for raw in handle:
                text = raw.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    truncated = True
                    continue
                if isinstance(payload, dict):
                    events.append(LedgerEvent.from_dict(payload))
        return events, truncated


def known_trains(store: JsonStore) -> list[str]:
    """列出落盘上存在流水的全部列车标识（升序）。"""

    root = os.path.join(store.root, "pq", "events")
    if not os.path.isdir(root):
        return []
    found = [
        train_id
        for name in os.listdir(root)
        if name.endswith(".jsonl") for train_id in [_unsafe(name)] if train_id
    ]
    return sorted(found)


def verify_all(store: JsonStore, train_ids: Iterable[str] | None = None) -> dict[str, int]:
    """校验一列（或全部）列车的哈希链，返回 ``{列车: 通过条数}``。"""

    ids = list(train_ids) if train_ids is not None else known_trains(store)
    return {train_id: HashLedger(store, train_id).verify() for train_id in ids}

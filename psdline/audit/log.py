"""审计流水写入。"""

from __future__ import annotations

import json

from ..clock import Clock
from ..errors import RailError
from ..eventbus import Event, EventBus
from ..ids import IdFactory
from ..store import JsonlJournal
from .entry import AuditEntry


class AuditLog:
    """审计流水：内存索引 + 分段 JSONL 落盘。"""

    def __init__(self, clock: Clock, bus: EventBus, journal: JsonlJournal) -> None:
        self._clock = clock
        self._bus = bus
        self._journal = journal
        self._ids = IdFactory("audit")
        self._index: list[AuditEntry] = []
        # 幂等键（例如优先级门控哈希链的事件哈希）→ 已落盘审计条目。重放/补镜像时
        # 同一条事实不会在平台审计里写第二遍。
        self._idempotency: dict[str, AuditEntry] = {}
        self._idempotency_seed = 0

    @property
    def journal(self) -> JsonlJournal:
        return self._journal

    @property
    def ids(self) -> IdFactory:
        return self._ids

    def record(
        self,
        kind: str,
        train_id: str,
        station_id: str,
        at: float | None = None,
        **detail: object,
    ) -> AuditEntry:
        """写入一条审计流水。"""

        return self._write(kind, train_id, station_id, None, at, detail)

    def record_once(
        self,
        idempotency_key: str,
        kind: str,
        train_id: str,
        station_id: str,
        at: float | None = None,
        **detail: object,
    ) -> AuditEntry | None:
        """按幂等键写审计；键已存在时原样返回既有条目（返回 ``None`` 表示新写）。

        用于把其他只追加流水（如优先级门控哈希链）镜像进平台审计：崩溃重放后
        重放边界不会产生重复审计。
        """

        if not idempotency_key:
            raise RailError("审计幂等键为空", component="audit")
        existing = self._idempotency.get(idempotency_key)
        if existing is not None:
            return existing
        self._write(kind, train_id, station_id, idempotency_key, at, detail)
        return None

    def _write(
        self,
        kind: str,
        train_id: str,
        station_id: str,
        idempotency_key: str | None,
        at: float | None,
        detail: dict,
    ) -> AuditEntry:
        if not kind:
            raise RailError("审计动作类型为空", component="audit")
        entry = AuditEntry(
            id=self._ids.next(),
            kind=kind,
            train_id=train_id,
            station_id=station_id,
            at=self._clock.now() if at is None else at,
            detail={
                key: value
                for key, value in detail.items()
                if value is not None and not isinstance(value, (dict, list))
            },
        )
        record = entry.as_dict()
        if idempotency_key is not None:
            record["idempotency_key"] = idempotency_key
        self._journal.append(record)
        self._index.append(entry)
        if idempotency_key is not None:
            self._idempotency[idempotency_key] = entry
        self._bus.publish(
            Event.of(
                "audit",
                "audit",
                kind,
                severity="notice",
                train_id=train_id,
                station_id=station_id,
            )
        )
        return entry

    def seed_idempotency(self) -> int:
        """从落盘流水重建幂等键索引，返回恢复出的键数量。

        新进程启动后，旧审计流水里带幂等键的条目仍然算数，重放镜像不会重复写。
        """

        count = 0
        for raw in self._journal.read_all():
            if not isinstance(raw, dict):
                continue
            key = raw.get("idempotency_key")
            if key:
                self._idempotency.setdefault(str(key), AuditEntry.from_dict(raw))
                count += 1
        self._idempotency_seed = count
        return count

    def list(self) -> list[AuditEntry]:
        """返回内存里保留的审计条目。"""

        return list(self._index)

    def replay(self, limit: int = 100_000) -> list[AuditEntry]:
        """从落盘流水回放审计条目。"""

        records = self._journal.read_all()[:limit]
        return [AuditEntry.from_dict(item) for item in records if isinstance(item, dict)]

    def segments(self) -> list[list[dict]]:
        """按段返回落盘流水，供巡检逐段核对。"""

        return [
            self._journal.read_segment(index)
            for index in range(len(self._journal.segments()))
        ]

    def summary(self) -> dict:
        """返回审计用量与动作分布。"""

        entries = self.list()
        return {
            "entries": len(entries),
            "journal_records": self._journal.count(),
            "segment": self._journal.active_segment(),
            "segments": len(self._journal.segments()),
            "segment_sizes": [len(items) for items in self.segments()],
            "by_kind": count_kinds(entries),
        }

    def dump(self) -> str:
        """把内存索引序列化成一行 JSON，供状态接口内嵌。"""

        return json.dumps([entry.as_dict() for entry in self.list()], ensure_ascii=False)


def count_kinds(entries: list[AuditEntry]) -> dict[str, int]:
    """按动作类型统计条数。"""

    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.kind] = counts.get(entry.kind, 0) + 1
    return counts

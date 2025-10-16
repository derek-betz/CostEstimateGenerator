"""Persistent state management for memo retrieval."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


@dataclass
class MemoRecord:
    memo_id: str
    url: str
    checksum: str
    downloaded_at: str
    filename: str
    processed: bool = False
    processed_at: Optional[str] = None
    summary_path: Optional[str] = None
    approved: bool = False
    approved_at: Optional[str] = None


@dataclass
class MemoState:
    path: Path
    last_checked: Optional[str] = None
    memos: Dict[str, MemoRecord] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "MemoState":
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
        else:
            raw = {"last_checked": None, "memos": {}}

        memos = {
            memo_id: MemoRecord(memo_id=memo_id, **data)
            for memo_id, data in raw.get("memos", {}).items()
        }
        return cls(path=path, last_checked=raw.get("last_checked"), memos=memos)

    def save(self) -> None:
        data = {
            "last_checked": self.last_checked,
            "memos": {memo_id: self._record_to_dict(record) for memo_id, record in self.memos.items()},
        }
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def update_last_checked(self, timestamp: datetime | None = None) -> None:
        ts = timestamp or datetime.now().astimezone()
        self.last_checked = ts.strftime(ISO_FORMAT)

    def register_memo(self, record: MemoRecord) -> None:
        self.memos[record.memo_id] = record

    @staticmethod
    def _record_to_dict(record: MemoRecord) -> dict:
        data = {
            "url": record.url,
            "checksum": record.checksum,
            "downloaded_at": record.downloaded_at,
            "filename": record.filename,
            "processed": record.processed,
        }
        if record.processed_at:
            data["processed_at"] = record.processed_at
        if record.summary_path:
            data["summary_path"] = record.summary_path
        if record.approved:
            data["approved"] = record.approved
        if record.approved_at:
            data["approved_at"] = record.approved_at
        return data


__all__ = ["MemoState", "MemoRecord", "ISO_FORMAT"]

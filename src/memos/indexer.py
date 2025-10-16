"""Maintain consolidated memo index after approval."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

from .state import MemoRecord

LOGGER = logging.getLogger(__name__)


@dataclass
class IndexEntry:
    memo_id: str
    summary_path: str
    checksum: str


class MemoIndexer:
    """Updates the memo index file."""

    def __init__(self, index_path: Path) -> None:
        self.index_path = index_path

    def update(self, records: Iterable[MemoRecord]) -> None:
        index = self._load()
        updated = False
        for record in records:
            if not record.summary_path:
                continue
            existing = next((entry for entry in index if entry["memo_id"] == record.memo_id), None)
            data = {
                "memo_id": record.memo_id,
                "summary_path": record.summary_path,
                "checksum": record.checksum,
            }
            if existing:
                if existing != data:
                    LOGGER.debug("Updating index entry for %s", record.memo_id)
                    existing.update(data)
                    updated = True
            else:
                LOGGER.debug("Adding index entry for %s", record.memo_id)
                index.append(data)
                updated = True
        if updated:
            self._save(index)

    def _load(self) -> List[dict]:
        if not self.index_path.exists():
            return []
        with self.index_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw.get("memos", []) if isinstance(raw, dict) else raw

    def _save(self, entries: List[dict]) -> None:
        payload = {"memos": sorted(entries, key=lambda e: e["memo_id"])}
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        with self.index_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        LOGGER.info("Updated memo index at %s", self.index_path)


__all__ = ["MemoIndexer", "IndexEntry"]

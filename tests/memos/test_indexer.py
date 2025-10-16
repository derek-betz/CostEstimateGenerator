from __future__ import annotations

import json
from pathlib import Path

from memos.indexer import MemoIndexer
from memos.state import MemoRecord


def test_indexer_updates(tmp_path: Path) -> None:
    index_path = tmp_path / "index.json"
    indexer = MemoIndexer(index_path)

    record = MemoRecord(
        memo_id="memo-1",
        url="https://example.com/memo.pdf",
        checksum="abc",
        downloaded_at="2024-01-01T00:00:00+0000",
        filename="memo.pdf",
        processed=True,
        summary_path="/tmp/memo.json",
    )
    indexer.update([record])
    data = json.loads(index_path.read_text(encoding="utf-8"))
    assert data["memos"][0]["summary_path"].startswith("/")

    record.summary_path = "C:/windows/style/path.json"
    record.checksum = "def"
    indexer.update([record])
    data = json.loads(index_path.read_text(encoding="utf-8"))
    assert data["memos"][0]["checksum"] == "def"
    assert "\\" not in data["memos"][0]["summary_path"]

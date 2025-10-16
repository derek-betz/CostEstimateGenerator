from __future__ import annotations

import hashlib
import hashlib
from pathlib import Path
from typing import Callable

import pytest
from reportlab.pdfgen import canvas

from memos.config import MemoConfig
from memos.state import MemoRecord, MemoState


@pytest.fixture
def memo_config(tmp_path: Path) -> MemoConfig:
    storage_root = tmp_path / "refs"
    raw_dir = storage_root / "raw"
    processed_dir = storage_root / "processed"
    digests_dir = storage_root / "digests"
    state_file = storage_root / "state.json"
    index_file = storage_root / "index.json"
    schema_source = Path("references/memos/schema/processed.schema.json")
    schema_target = storage_root / "schema" / "processed.schema.json"
    schema_target.parent.mkdir(parents=True, exist_ok=True)
    schema_target.write_text(schema_source.read_text(encoding="utf-8"), encoding="utf-8")

    config = MemoConfig.from_dict(
        {
            "memo_page_url": "https://example.com/memos",
            "storage_root": str(storage_root),
            "raw_directory": str(raw_dir),
            "processed_directory": str(processed_dir),
            "digests_directory": str(digests_dir),
            "state_file": str(state_file),
            "index_file": str(index_file),
            "notification": {
                "enabled": False,
                "recipients": [],
            },
        }
    )
    config.ensure_directories()
    return config


@pytest.fixture
def memo_state(memo_config: MemoConfig) -> MemoState:
    return MemoState.load(memo_config.state_file)


@pytest.fixture
def pdf_factory(tmp_path: Path) -> Callable[[str, str], Path]:
    def _create(filename: str, text: str) -> Path:
        pdf_path = tmp_path / filename
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        canv = canvas.Canvas(str(pdf_path))
        y = 800
        for line in text.splitlines():
            canv.drawString(72, y, line)
            y -= 18
        canv.save()
        return pdf_path

    return _create


@pytest.fixture
def memo_record_factory(memo_config: MemoConfig) -> Callable[[str, str], MemoRecord]:
    def _create(memo_id: str, text: str) -> MemoRecord:
        filename = f"{memo_id}.pdf"
        pdf_path = memo_config.raw_directory / filename
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        canv = canvas.Canvas(str(pdf_path))
        y = 800
        for line in text.splitlines():
            canv.drawString(72, y, line)
            y -= 18
        canv.save()
        content = pdf_path.read_bytes()
        checksum = hashlib.sha256(content).hexdigest()
        return MemoRecord(
            memo_id=memo_id,
            url=f"https://example.com/{filename}",
            checksum=checksum,
            downloaded_at="2024-01-01T00:00:00+0000",
            filename=filename,
        )

    return _create

from __future__ import annotations

import json
import json
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")
from jsonschema import ValidationError

from costest.memos.parser import MemoParser
from costest.memos.state import MemoRecord


def test_parse_creates_outputs(memo_config, memo_state, memo_record_factory) -> None:
    text = (
        "INDOT Memo Title\n"
        "Effective: 2024-05-01\n"
        "This memo updates pay item 12345 near Item references.\n"
        "Replacement item code: 54321\n"
        "Obsolete item code: 11111\n"
        "Section 620 applies. Total cost $1,234.50."
    )
    record = memo_record_factory("memo-2024-05", text)

    parser = MemoParser(memo_config, memo_state)
    parsed = parser.parse_new_memos([record])
    assert len(parsed) == 1
    parsed_record = parsed[0]
    assert memo_state.memos[record.memo_id].processed
    assert memo_state.memos[record.memo_id].summary_path == parsed_record.summary_path.as_posix()

    summary_data = json.loads(parsed_record.summary_path.read_text(encoding="utf-8"))
    assert summary_data["metadata"]["title"].startswith("INDOT Memo Title")
    assert summary_data["metadata"]["effective_date"] == "2024-05-01"
    assert "54321" in summary_data["metadata"]["replacement_item_codes"]
    assert "11111" in summary_data["metadata"]["obsolete_item_codes"]
    assert summary_data["highlights"]["pay_items"]
    assert summary_data["highlights"]["keywords_present"]

    digest_text = parsed_record.digest_path.read_text(encoding="utf-8")
    assert "Memo ID" in digest_text


def test_parse_validation_failure(monkeypatch, memo_config, memo_state, memo_record_factory) -> None:
    record = memo_record_factory("memo-err", "Simple text")
    parser = MemoParser(memo_config, memo_state)
    if parser._validator is None:  # pragma: no cover - schema missing only when jsonschema absent
        pytest.skip("jsonschema not available")

    def fail_validate(self, payload):
        raise ValidationError("bad schema")

    monkeypatch.setattr(type(parser._validator), "validate", fail_validate, raising=False)

    parsed = parser.parse_new_memos([record])
    assert parsed == []
    assert memo_state.memos[record.memo_id].processed is False
    assert "Schema validation failed" in memo_state.memos[record.memo_id].error


def test_pay_item_guard_limits_results(memo_config, memo_state, memo_record_factory) -> None:
    parser = MemoParser(memo_config, memo_state)
    parser.config.patterns.pay_item_limit = 3
    parser.config.patterns.pay_item_frequency_guard = 5
    text = " ".join([f"Pay Item {1000 + i}" for i in range(20)])
    results = parser._extract_pay_items(text)
    assert len(results) <= 3


def test_metadata_extraction(memo_config, memo_state, memo_record_factory) -> None:
    text = (
        "Major Update Memo\n"
        "EFFECTIVE: June 1, 2024\n"
        "Replacement item 60001 replaces 50000.\n"
        "Obsolete Item 40000 will no longer be used. Section 601 applies."
    )
    record = memo_record_factory("meta-test", text)
    parser = MemoParser(memo_config, memo_state)
    parsed = parser.parse_new_memos([record])[0]
    metadata = parsed.metadata
    assert metadata["title"].startswith("Major Update")
    assert metadata["effective_date"].lower().startswith("june")
    assert "60001" in metadata["replacement_item_codes"]
    assert "40000" in metadata["obsolete_item_codes"]
    assert "601" in metadata["affected_spec_sections"]


def test_parse_missing_pdf_records_error(memo_config, memo_state) -> None:
    record = MemoRecord(
        memo_id="missing-pdf",
        url="https://example.com/missing.pdf",
        checksum="deadbeef",
        downloaded_at="2024-01-01T00:00:00+0000",
        filename="missing.pdf",
    )
    parser = MemoParser(memo_config, memo_state)
    parsed = parser.parse_new_memos([record])
    assert parsed == []
    state_record = memo_state.memos[record.memo_id]
    assert state_record.processed is False
    assert state_record.error
    assert "not found" in state_record.error.lower()


def test_parse_falls_back_to_matching_pdf(memo_config, memo_state, memo_record_factory) -> None:
    text = (
        "Memo Title\n"
        "Effective 2024-06-01\n"
        "Pay Item 55555 with updates.\n"
    )
    record = memo_record_factory("memo-fallback", text)
    original_path = memo_config.raw_directory / record.filename
    fallback_path = memo_config.raw_directory / "2024-06-memo-fallback-update.pdf"
    original_path.rename(fallback_path)
    record.filename = "missing-name.pdf"

    parser = MemoParser(memo_config, memo_state)
    parsed = parser.parse_new_memos([record])
    assert len(parsed) == 1
    parsed_record = parsed[0]
    assert parsed_record.source_pdf == fallback_path
    assert memo_state.memos[record.memo_id].processed

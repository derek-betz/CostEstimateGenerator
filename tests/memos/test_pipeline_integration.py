from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")
from jsonschema import Draft7Validator

from scripts.memos import run_pipeline


class ResponseStub:
    def __init__(self, data: bytes, url: str):
        self._data = data
        self._url = url

    def read(self) -> bytes:
        return self._data

    def geturl(self) -> str:
        return self._url

    def __enter__(self) -> "ResponseStub":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


@pytest.fixture
def pipeline_setup(tmp_path: Path, pdf_factory):
    root = tmp_path / "refs"
    root.mkdir(parents=True, exist_ok=True)
    schema_source = Path("references/memos/schema/processed.schema.json")
    schema_target = root / "schema" / "processed.schema.json"
    schema_target.parent.mkdir(parents=True, exist_ok=True)
    schema_target.write_text(schema_source.read_text(encoding="utf-8"), encoding="utf-8")

    html_listing = """
    <html><body>
    <a href="memo-1.pdf">Memo 2024-03</a>
    </body></html>
    """.strip()
    pdf_path = pdf_factory("memo-1.pdf", "Memo Title\nEffective: 2024-03-01\nPay Item 12345")
    pdf_bytes = pdf_path.read_bytes()

    return {
        "root": root,
        "html": html_listing.encode("utf-8"),
        "pdf": pdf_bytes,
    }


def make_config(root: Path, state_name: str, *, notifications_enabled: bool) -> Path:
    config_path = root.parent / f"config-{state_name}.json"
    config = {
        "memo_page_url": "https://example.com/listing",
        "storage_root": str(root),
        "raw_directory": str(root / "raw"),
        "processed_directory": str(root / "processed"),
        "digests_directory": str(root / "digests"),
        "state_file": str(root / f"{state_name}.json"),
        "notification": {
            "enabled": notifications_enabled,
            "sender": "sender@example.com",
            "recipients": ["r@example.com"],
            "smtp": {"host": "smtp.example.com"},
        },
        "approval": {"method": "none"},
    }
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return config_path


def test_pipeline_integration(monkeypatch, capsys, pipeline_setup):
    html_bytes = pipeline_setup["html"]
    pdf_bytes = pipeline_setup["pdf"]
    root = pipeline_setup["root"]

    def fake_urlopen(request, timeout=None):
        url = getattr(request, "full_url", request)
        if url.endswith("listing"):
            return ResponseStub(html_bytes, "https://example.com/listing")
        return ResponseStub(pdf_bytes, "https://example.com/memo-1.pdf")

    monkeypatch.setattr("costest.memos.scraper.urlopen", fake_urlopen)

    call_log = []

    def fake_notify(self, subject, body, attachments, *, force=False):
        call_log.append({"subject": subject, "force": force, "attachments": list(attachments)})
        return True

    monkeypatch.setattr("costest.memos.workflow.MemoNotifier.notify", fake_notify, raising=False)

    config_path = make_config(root, "state", notifications_enabled=True)
    state_path = Path(json.loads(config_path.read_text())["state_file"])

    argv = ["run_pipeline.py", "--config", str(config_path), "--state", str(state_path)]
    monkeypatch.setattr(sys, "argv", argv)
    run_pipeline.main()
    output = capsys.readouterr().out
    summary = json.loads(output)
    assert summary["parsed_count"] == 1
    assert call_log

    state_data = json.loads(state_path.read_text(encoding="utf-8"))
    memo_entry = next(iter(state_data["memos"].values()))
    assert memo_entry["processed"] is True
    processed_path = Path(memo_entry["summary_path"])
    assert processed_path.exists()
    digest_path = root / "digests" / f"{processed_path.stem}.md"
    assert digest_path.exists()

    validator = Draft7Validator(json.loads((root / "schema" / "processed.schema.json").read_text(encoding="utf-8")))
    validator.validate(json.loads(processed_path.read_text(encoding="utf-8")))

    # Second run with notifications disabled should skip notify
    config_path_disabled = make_config(root, "state-disabled", notifications_enabled=False)
    argv2 = ["run_pipeline.py", "--config", str(config_path_disabled), "--state", str(root / "state-disabled.json"), "--no-notify"]
    monkeypatch.setattr(sys, "argv", argv2)
    call_log.clear()
    run_pipeline.main()
    summary2 = json.loads(capsys.readouterr().out)
    assert summary2["parsed_count"] == 1
    assert not call_log

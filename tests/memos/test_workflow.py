from __future__ import annotations

from pathlib import Path

from memos.config import SMTPConfig
from memos.parser import ParsedMemo
from memos.scraper import ScrapedMemo
from memos.state import MemoRecord
from memos.workflow import MemoWorkflow


def test_workflow_metrics(monkeypatch, memo_config, memo_state) -> None:
    workflow = MemoWorkflow(memo_config, memo_state)

    memo_record = MemoRecord(
        memo_id="memo-1",
        url="https://example.com/memo.pdf",
        checksum="abc",
        downloaded_at="2024-01-01T00:00:00+0000",
        filename="memo.pdf",
    )
    parsed_memo = ParsedMemo(
        memo_id="memo-1",
        source_pdf=memo_config.raw_directory / "memo.pdf",
        summary_path=memo_config.processed_directory / "memo-1.json",
        digest_path=memo_config.digests_directory / "memo-1.md",
        highlights={},
        metadata={},
    )

    workflow.scraper.fetch_listing = lambda: [ScrapedMemo("memo-1", "url", "file.pdf")]  # type: ignore[assignment]
    workflow.scraper.download_new_memos = lambda memos: [memo_record]  # type: ignore[assignment]
    workflow.parser.parse_new_memos = lambda records: [parsed_memo]  # type: ignore[assignment]
    workflow.parser._last_failures = 2  # type: ignore[attr-defined]

    notifications = {"sent": False}

    def fake_notify(*args, **kwargs):
        notifications["sent"] = True
        return True

    workflow.notifier.notify = fake_notify  # type: ignore[assignment]
    memo_config.notification.enabled = True
    memo_config.notification.sender = "sender@example.com"
    memo_config.notification.recipients = ["r@example.com"]
    memo_config.notification.smtp = SMTPConfig(host="smtp.example.com", port=587)

    result = workflow.run(notify=True)

    assert result.fetched_count == 1
    assert result.downloaded_count == 1
    assert result.parsed_count == 1
    assert result.failed_parse_count == 2
    assert result.notified is True
    assert notifications["sent"]


def test_workflow_skips_notification_when_no_parsed(monkeypatch, memo_config, memo_state) -> None:
    workflow = MemoWorkflow(memo_config, memo_state)
    workflow.scraper.fetch_listing = lambda: []  # type: ignore[assignment]
    workflow.scraper.download_new_memos = lambda memos: []  # type: ignore[assignment]
    workflow.parser.parse_new_memos = lambda records: []  # type: ignore[assignment]

    notifications = {"sent": False}

    def fake_notify(*args, **kwargs):
        notifications["sent"] = True
        return True

    workflow.notifier.notify = fake_notify  # type: ignore[assignment]

    result = workflow.run(notify=True)
    assert result.parsed_count == 0
    assert result.notified is False
    assert not notifications["sent"]

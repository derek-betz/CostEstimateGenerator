"""High-level workflow orchestration for memo automation."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List

from .config import MemoConfig
from .notifier import MemoNotifier
from .parser import MemoParser, ParsedMemo
from .scraper import MemoScraper, ScrapedMemo
from .state import MemoRecord, MemoState

LOGGER = logging.getLogger(__name__)


@dataclass
class WorkflowResult:
    fetched: List[ScrapedMemo]
    downloaded: List[MemoRecord]
    parsed: List[ParsedMemo]
    failed_parse_count: int = 0
    notified: bool = False
    fetched_count: int = field(init=False)
    downloaded_count: int = field(init=False)
    parsed_count: int = field(init=False)

    def __post_init__(self) -> None:
        self.fetched_count = len(self.fetched)
        self.downloaded_count = len(self.downloaded)
        self.parsed_count = len(self.parsed)


class MemoWorkflow:
    """Coordinates scraping, parsing, and notification."""

    def __init__(self, config: MemoConfig, state: MemoState) -> None:
        self.config = config
        self.state = state
        self.scraper = MemoScraper(config, state)
        self.parser = MemoParser(config, state)
        self.notifier = MemoNotifier(config)

    def run(self, notify: bool = True) -> WorkflowResult:
        LOGGER.info("Starting memo workflow")
        self.config.ensure_directories()
        listing = self.scraper.fetch_listing()
        downloaded = self.scraper.download_new_memos(listing)
        parsed = self.parser.parse_new_memos(downloaded)
        failed_parse = getattr(self.parser, "last_failed_count", 0)
        self.state.update_last_checked()
        self.state.save()

        notification_sent = False
        if notify and parsed:
            notification_sent = self._send_notification(parsed)

        result = WorkflowResult(
            fetched=listing,
            downloaded=downloaded,
            parsed=parsed,
            failed_parse_count=failed_parse,
            notified=notification_sent,
        )
        LOGGER.info(
            "Workflow summary: fetched=%d downloaded=%d parsed=%d failed=%d notified=%s",
            result.fetched_count,
            result.downloaded_count,
            result.parsed_count,
            result.failed_parse_count,
            "yes" if result.notified else "no",
        )
        return result

    def _send_notification(self, parsed: Iterable[ParsedMemo]) -> bool:
        lines = ["New INDOT Active Design Memos detected:"]
        attachments: List[Path] = []
        for memo in parsed:
            lines.append(f"- {memo.memo_id}: {memo.digest_path}")
            attachments.append(memo.digest_path)
        lines.append("\nPlease review the attached digests. Reply with the configured approval phrase to approve ingestion.")
        subject = f"{len(attachments)} new INDOT memo(s) ready for review"
        return self.notifier.notify(
            subject=subject,
            body="\n".join(lines),
            attachments=attachments,
        )


__all__ = ["MemoWorkflow", "WorkflowResult"]

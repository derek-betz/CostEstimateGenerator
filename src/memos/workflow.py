"""High-level workflow orchestration for memo automation."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from .ai import AIReview, MemoAIReviewer
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
    ai_reviews: List[AIReview]


class MemoWorkflow:
    """Coordinates scraping, parsing, and notification."""

    def __init__(self, config: MemoConfig, state: MemoState) -> None:
        self.config = config
        self.state = state
        self.scraper = MemoScraper(config, state)
        self.parser = MemoParser(config, state)
        self.notifier = MemoNotifier(config)
        self.ai_reviewer = MemoAIReviewer(config)

    def run(self, notify: bool = True) -> WorkflowResult:
        LOGGER.info("Starting memo workflow")
        self.config.ensure_directories()
        listing = self.scraper.fetch_listing()
        downloaded = self.scraper.download_new_memos(listing)
        parsed = self.parser.parse_new_memos(downloaded)
        ai_reviews = self.ai_reviewer.review(parsed)
        if ai_reviews:
            self._update_state_with_ai(ai_reviews)
        self.state.update_last_checked()
        self.state.save()

        if notify and parsed:
            self._send_notification(parsed, ai_reviews)

        return WorkflowResult(
            fetched=listing,
            downloaded=downloaded,
            parsed=parsed,
            ai_reviews=ai_reviews,
        )

    def _update_state_with_ai(self, reviews: Iterable[AIReview]) -> None:
        for review in reviews:
            record = self.state.memos.get(review.memo_id)
            if record:
                record.ai_digest_path = str(review.analysis_path)

    def _send_notification(self, parsed: Iterable[ParsedMemo], ai_reviews: Iterable[AIReview]) -> None:
        memo_list = list(parsed)
        ai_lookup: Dict[str, AIReview] = {review.memo_id: review for review in ai_reviews}
        lines = ["New INDOT Active Design Memos detected:"]
        attachments: List[Path] = []
        for memo in memo_list:
            lines.append(f"- {memo.memo_id}: {memo.digest_path}")
            if memo.digest_path not in attachments:
                attachments.append(memo.digest_path)
            if memo.memo_id in ai_lookup:
                review = ai_lookup[memo.memo_id]
                lines.append(f"  AI insights: {review.analysis_path}")
                if review.analysis_path not in attachments:
                    attachments.append(review.analysis_path)
        lines.append(
            "\nPlease review the attached digests. Reply with the configured approval phrase to approve ingestion."
        )
        subject = f"{len(memo_list)} new INDOT memo(s) ready for review"
            self._send_notification(parsed)

        return WorkflowResult(fetched=listing, downloaded=downloaded, parsed=parsed)

    def _send_notification(self, parsed: Iterable[ParsedMemo]) -> None:
        lines = ["New INDOT Active Design Memos detected:"]
        attachments: List[Path] = []
        for memo in parsed:
            lines.append(f"- {memo.memo_id}: {memo.digest_path}")
            attachments.append(memo.digest_path)
        lines.append("\nPlease review the attached digests. Reply with the configured approval phrase to approve ingestion.")
        subject = f"{len(attachments)} new INDOT memo(s) ready for review"
        self.notifier.notify(subject=subject, body="\n".join(lines), attachments=attachments)


__all__ = ["MemoWorkflow", "WorkflowResult"]

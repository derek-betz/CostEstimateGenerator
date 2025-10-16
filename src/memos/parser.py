"""PDF parsing utilities for memo extraction."""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from PyPDF2 import PdfReader

from .config import MemoConfig
from .state import ISO_FORMAT, MemoRecord, MemoState

LOGGER = logging.getLogger(__name__)

PAY_ITEM_PATTERN = re.compile(r"\b(?P<item>\d{4,6})\b")
SPEC_PATTERN = re.compile(r"Section\s+(?P<section>\d{3})")
DOLLAR_PATTERN = re.compile(r"\$\s?(?P<amount>[0-9,.]+)")
KEYWORDS = [
    "pay item",
    "unit price",
    "specification",
    "standard drawing",
    "change",
    "update",
]


@dataclass
class ParsedMemo:
    memo_id: str
    source_pdf: Path
    summary_path: Path
    digest_path: Path
    highlights: Dict[str, List[str]]
    metadata: Dict[str, str]


class MemoParser:
    """Parses memo PDFs into structured summaries."""

    def __init__(self, config: MemoConfig, state: MemoState) -> None:
        self.config = config
        self.state = state

    def parse_new_memos(self, records: Iterable[MemoRecord]) -> List[ParsedMemo]:
        parsed: List[ParsedMemo] = []
        for record in records:
            if record.processed:
                LOGGER.debug("Skipping already processed memo %s", record.memo_id)
                continue
            try:
                result = self._parse_single(record)
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.exception("Failed to parse memo %s: %s", record.memo_id, exc)
                continue
            self._mark_processed(record, result)
            parsed.append(result)
        return parsed

    def _parse_single(self, record: MemoRecord) -> ParsedMemo:
        target_name = record.filename if record.filename.endswith(".pdf") else f"{record.memo_id}.pdf"
        pdf_path = self.config.raw_directory / target_name
        if not pdf_path.exists():
            # Fallback to scanning raw directory for matching checksum
            candidates = list(self.config.raw_directory.glob(f"*{record.memo_id}*.pdf"))
            if candidates:
                pdf_path = candidates[0]
            else:
                raise FileNotFoundError(f"PDF for memo {record.memo_id} not found")

        LOGGER.info("Extracting text from %s", pdf_path)
        text = self._extract_text(pdf_path)
        highlights = self._extract_highlights(text)

        metadata = {
            "memo_id": record.memo_id,
            "source_pdf": str(pdf_path),
            "checksum": record.checksum,
            "extracted_at": datetime.now().astimezone().strftime(ISO_FORMAT),
            "character_count": str(len(text)),
        }

        summary_path = self.config.processed_directory / f"{record.memo_id}.json"
        digest_path = self.config.digests_directory / f"{record.memo_id}.md"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        digest_path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "metadata": metadata,
            "highlights": highlights,
            "snippets": self._collect_snippets(text, highlights),
        }

        with summary_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        LOGGER.info("Wrote structured summary to %s", summary_path)

        digest_content = self._render_digest(payload)
        digest_path.write_text(digest_content, encoding="utf-8")
        LOGGER.info("Wrote digest to %s", digest_path)

        return ParsedMemo(
            memo_id=record.memo_id,
            source_pdf=pdf_path,
            summary_path=summary_path,
            digest_path=digest_path,
            highlights=highlights,
            metadata=metadata,
        )

    def _mark_processed(self, record: MemoRecord, parsed: ParsedMemo) -> None:
        record.processed = True
        record.processed_at = datetime.now().astimezone().strftime(ISO_FORMAT)
        record.summary_path = str(parsed.summary_path)
        self.state.register_memo(record)

    def _extract_highlights(self, text: str) -> Dict[str, List[str]]:
        pay_items = sorted(set(PAY_ITEM_PATTERN.findall(text)))
        specs = sorted(set(SPEC_PATTERN.findall(text)))
        amounts = sorted(set(DOLLAR_PATTERN.findall(text)))

        keyword_hits = []
        lowered = text.lower()
        for keyword in KEYWORDS:
            if keyword in lowered:
                keyword_hits.append(keyword)

        return {
            "pay_items": pay_items,
            "spec_sections": specs,
            "dollar_amounts": amounts,
            "keywords_present": keyword_hits,
        }

    def _collect_snippets(self, text: str, highlights: Dict[str, List[str]], window: int = 180) -> List[str]:
        snippets: List[str] = []
        lowered = text.lower()
        for keyword in KEYWORDS:
            idx = lowered.find(keyword)
            if idx == -1:
                continue
            start = max(0, idx - window)
            end = min(len(text), idx + window)
            snippets.append(text[start:end].strip())

        # Additional snippets around pay items
        for item in highlights.get("pay_items", []):
            pattern = re.compile(rf"(.{{0,{window}}}{item}.{{0,{window}}})", re.IGNORECASE)
            if match := pattern.search(text):
                snippets.append(match.group(1).strip())
        return snippets

    def _render_digest(self, payload: Dict[str, object]) -> str:
        metadata = payload["metadata"]
        highlights = payload["highlights"]
        snippets = payload["snippets"]
        lines = ["# Memo Summary", "", f"**Memo ID:** {metadata['memo_id']}"]
        lines.append(f"**Checksum:** {metadata['checksum']}")
        lines.append(f"**Extracted At:** {metadata['extracted_at']}")
        lines.append("\n## Highlights")
        for key, values in highlights.items():
            lines.append(f"- **{key.replace('_', ' ').title()}**: {', '.join(values) if values else 'None detected'}")
        lines.append("\n## Notable Snippets")
        if snippets:
            for snippet in snippets:
                lines.append(f"- {snippet}")
        else:
            lines.append("- No keyword snippets identified.")
        return "\n".join(lines)

    def _extract_text(self, pdf_path: Path) -> str:
        reader = PdfReader(str(pdf_path))
        text_parts: List[str] = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            text_parts.append(page_text)
        return "\n".join(text_parts)


__all__ = ["MemoParser", "ParsedMemo"]

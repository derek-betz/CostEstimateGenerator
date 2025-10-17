"""PDF parsing utilities for memo extraction."""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from PyPDF2 import PdfReader

from .config import MemoConfig
from .state import ISO_FORMAT, MemoRecord, MemoState

try:  # pragma: no cover - optional dependency guard
    from jsonschema import Draft7Validator, ValidationError
except Exception:  # pragma: no cover - import fallback
    Draft7Validator = None  # type: ignore

    class ValidationError(Exception):  # type: ignore[override]
        pass

LOGGER = logging.getLogger(__name__)

SPEC_PATTERN = re.compile(r"Section\s+(?P<section>\d{3})", re.IGNORECASE)
DOLLAR_PATTERN = re.compile(r"\$\s?(?P<amount>[0-9,.]+)")
EFFECTIVE_PATTERN = re.compile(
    r"effective\s*[:\-]?\s*(?P<date>(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4})|(?:\w+\s+\d{1,2},\s+\d{4})|(?:\d{4}-\d{2}-\d{2}))",
    re.IGNORECASE,
)
REPLACEMENT_PATTERN = re.compile(
    r"(?:replacement|new)\s+(?:pay\s+item|item(?:\s+code)?|code)\s*[:\-]?\s*(?P<code>[0-9]{4,6}[A-Za-z]?)",
    re.IGNORECASE,
)
OBSOLETE_PATTERN = re.compile(
    r"(?:obsolete|retired|retire)\s+(?:pay\s+item|item(?:\s+code)?|code)\s*[:\-]?\s*(?P<code>[0-9]{4,6}[A-Za-z]?)",
    re.IGNORECASE,
)
REPLACES_PATTERN = re.compile(
    r"replac(?:es|ing)\s+(?P<code>[0-9]{4,6}[A-Za-z]?)",
    re.IGNORECASE,
)


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
        self._pay_item_pattern = re.compile(config.patterns.pay_item_regex, re.IGNORECASE)
        self._keywords = [kw.lower() for kw in config.patterns.keywords]
        self._schema_path = config.storage_root / "schema" / "processed.schema.json"
        self._validator = self._build_validator()
        self._last_failures = 0

    def parse_new_memos(self, records: Iterable[MemoRecord]) -> List[ParsedMemo]:
        parsed: List[ParsedMemo] = []
        failures = 0
        for record in records:
            if record.processed:
                LOGGER.debug("Skipping already processed memo %s", record.memo_id)
                continue
            try:
                result = self._parse_single(record)
            except ValidationError as exc:
                record.error = f"Schema validation failed: {exc.message}"
                record.processed = False
                self.state.register_memo(record)
                LOGGER.error("Schema validation failed for %s: %s", record.memo_id, exc)
                failures += 1
                continue
            except Exception as exc:  # pragma: no cover - defensive logging
                record.error = str(exc)
                record.processed = False
                self.state.register_memo(record)
                LOGGER.exception("Failed to parse memo %s: %s", record.memo_id, exc)
                failures += 1
                continue
            record.error = None
            self._mark_processed(record, result)
            parsed.append(result)
        self._last_failures = failures
        return parsed

    @property
    def last_failed_count(self) -> int:
        return self._last_failures

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

        metadata = self._build_metadata(record, pdf_path, text, highlights)

        summary_path = self.config.processed_directory / f"{record.memo_id}.json"
        digest_path = self.config.digests_directory / f"{record.memo_id}.md"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        digest_path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "metadata": metadata,
            "highlights": highlights,
            "snippets": self._collect_snippets(text, highlights),
        }

        self._validate_payload(record.memo_id, payload)

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
        record.summary_path = parsed.summary_path.as_posix()
        self.state.register_memo(record)

    def _extract_highlights(self, text: str) -> Dict[str, List[str]]:
        pay_items = self._extract_pay_items(text)
        spec_matches: List[str] = []
        for match in SPEC_PATTERN.finditer(text):
            try:
                section = match.group("section")
            except IndexError:  # pragma: no cover - defensive
                section = match.group(0)
            if section:
                spec_matches.append(section)
        specs = sorted(set(spec_matches))
        amounts = sorted(set(DOLLAR_PATTERN.findall(text)))

        keyword_hits = []
        lowered = text.lower()
        for keyword in self._keywords:
            if keyword in lowered:
                keyword_hits.append(keyword)

        return {
            "pay_items": pay_items,
            "spec_sections": specs,
            "dollar_amounts": amounts,
            "keywords_present": keyword_hits,
        }

    def _collect_snippets(
        self,
        text: str,
        highlights: Dict[str, List[str]],
        window: int = 180,
    ) -> List[str]:
        snippets: List[str] = []
        lowered = text.lower()
        for keyword in self._keywords:
            idx = lowered.find(keyword)
            if idx == -1:
                continue
            start = max(0, idx - window)
            end = min(len(text), idx + window)
            snippets.append(text[start:end].strip())

        # Additional snippets around pay items
        for item in highlights.get("pay_items", []):
            pattern = re.compile(rf"(.{{0,{window}}}{re.escape(item)}.{{0,{window}}})", re.IGNORECASE)
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

    def _extract_pay_items(self, text: str) -> List[str]:
        matches: List[tuple[str, bool]] = []
        for match in self._pay_item_pattern.finditer(text):
            groups = match.groupdict() if match.groupdict() else {}
            item = groups.get("item") or match.group(0)
            item = item.strip()
            if not item:
                continue
            context = text[max(0, match.start() - 25) : min(len(text), match.end() + 25)].lower()
            if self._is_false_positive_pay_item(item, context):
                continue
            has_keyword = any(keyword in context for keyword in self._keywords)
            matches.append((item, has_keyword))

        if not matches:
            return []

        unique_items = {item for item, _ in matches}
        if len(matches) <= self.config.patterns.pay_item_frequency_guard:
            return sorted(unique_items)

        limit = max(1, self.config.patterns.pay_item_limit)
        prioritized: List[str] = []
        seen: set[str] = set()
        for item, has_keyword in matches:
            if has_keyword and item not in seen:
                prioritized.append(item)
                seen.add(item)
                if len(prioritized) >= limit:
                    break

        if len(prioritized) < limit:
            for item, _ in Counter(item for item, _ in matches).most_common():
                if item not in seen:
                    prioritized.append(item)
                    seen.add(item)
                    if len(prioritized) >= limit:
                        break

        return prioritized

    @staticmethod
    def _is_false_positive_pay_item(item: str, context: str) -> bool:
        digits = re.sub(r"\D", "", item)
        if not digits:
            return True
        if len(digits) <= 3:
            return True
        if len(digits) >= 7 and "item" not in context:
            return True
        year = int(digits[:4]) if len(digits) >= 4 else None
        if year and 1900 <= year <= 2099 and "item" not in context:
            return True
        return False

    def _build_metadata(
        self,
        record: MemoRecord,
        pdf_path: Path,
        text: str,
        highlights: Dict[str, List[str]],
    ) -> Dict[str, str | Sequence[str]]:
        metadata: Dict[str, str | Sequence[str]] = {
            "memo_id": record.memo_id,
            "source_pdf": pdf_path.as_posix(),
            "checksum": record.checksum,
            "extracted_at": datetime.now().astimezone().strftime(ISO_FORMAT),
            "character_count": str(len(text)),
        }

        if title := self._extract_title(text):
            metadata["title"] = title
        if effective := self._extract_effective_date(text):
            metadata["effective_date"] = effective

        if highlights.get("spec_sections"):
            metadata["affected_spec_sections"] = highlights["spec_sections"]

        replacement_codes = self._extract_codes(text, REPLACEMENT_PATTERN)
        replacement_codes.update(self._extract_codes(text, REPLACES_PATTERN))
        obsolete_codes = self._extract_codes(text, OBSOLETE_PATTERN)

        if replacement_codes:
            metadata["replacement_item_codes"] = sorted(replacement_codes)
        if obsolete_codes:
            metadata["obsolete_item_codes"] = sorted(obsolete_codes)

        return metadata

    @staticmethod
    def _extract_title(text: str) -> Optional[str]:
        for line in text.splitlines():
            cleaned = line.strip()
            if len(cleaned) < 5:
                continue
            if re.search(r"[A-Za-z]{3}", cleaned):
                return cleaned
        return None

    @staticmethod
    def _extract_effective_date(text: str) -> Optional[str]:
        if match := EFFECTIVE_PATTERN.search(text):
            return match.group("date")
        return None

    @staticmethod
    def _extract_codes(text: str, pattern: re.Pattern[str]) -> set[str]:
        codes: set[str] = set()
        for match in pattern.finditer(text):
            code = match.group("code")
            if code:
                codes.add(code)
        return codes

    def _build_validator(self):
        if not Draft7Validator:
            LOGGER.debug("jsonschema is not available; skipping processed memo validation")
            return None
        if not self._schema_path.exists():
            LOGGER.debug("Processed memo schema not found at %s", self._schema_path)
            return None
        with self._schema_path.open("r", encoding="utf-8") as f:
            try:
                schema = json.load(f)
            except json.JSONDecodeError as exc:  # pragma: no cover - defensive
                LOGGER.error("Invalid processed memo schema at %s: %s", self._schema_path, exc)
                return None
        return Draft7Validator(schema)

    def _validate_payload(self, memo_id: str, payload: dict) -> None:
        if not self._validator:
            return
        try:
            self._validator.validate(payload)
        except ValidationError as exc:
            raise exc


__all__ = ["MemoParser", "ParsedMemo"]

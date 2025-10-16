"""Web scraper for the INDOT Active Design Memos page."""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from .config import MemoConfig
from .retry import CircuitBreaker, CircuitBreakerOpen, execute_with_retry
from .state import ISO_FORMAT, MemoRecord, MemoState

LOGGER = logging.getLogger(__name__)

PDF_PATTERN = re.compile(r"\.pdf$", re.IGNORECASE)
DATE_PATTERN = re.compile(r"(\b\d{4}\b)[^\d]{0,5}(\b(?:0?[1-9]|1[0-2])\b)")


@dataclass
class ScrapedMemo:
    memo_id: str
    url: str
    filename: str
    published_date: Optional[str] = None


class MemoLinkParser(HTMLParser):
    """HTML parser that collects PDF anchor tags."""

    def __init__(self) -> None:
        super().__init__()
        self.links: List[tuple[str, str]] = []
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, str]]) -> None:
        if tag.lower() != "a":
            return
        attr_dict = dict(attrs)
        href = attr_dict.get("href")
        if href and PDF_PATTERN.search(href):
            self._current_href = href
            self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._current_href:
            return
        text = "".join(self._current_text).strip()
        self.links.append((self._current_href, text))
        self._current_href = None
        self._current_text = []


class MemoScraper:
    """Scrapes memo links and downloads new files."""

    def __init__(self, config: MemoConfig, state: MemoState) -> None:
        self.config = config
        self.state = state
        self._listing_breaker = CircuitBreaker(self.config.http_retry.circuit_breaker_failures)
        self._download_breaker = CircuitBreaker(self.config.download_retry.circuit_breaker_failures)

    def fetch_listing(self) -> List[ScrapedMemo]:
        LOGGER.info("Fetching memo listing: %s", self.config.memo_page_url)
        request = Request(self.config.memo_page_url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            html, base_url = execute_with_retry(
                lambda timeout: _read_listing(request, timeout),
                policy=self.config.http_retry,
                description="memo listing fetch",
                logger=LOGGER,
                breaker=self._listing_breaker,
            )
        except CircuitBreakerOpen:
            LOGGER.error("HTTP circuit breaker open; skipping memo listing fetch")
            return []
        except Exception as exc:  # pragma: no cover - network failures
            LOGGER.error("Unable to fetch memo listing: %s", exc)
            return []

        parser = MemoLinkParser()
        parser.feed(html)
        LOGGER.debug("Found %d PDF links", len(parser.links))

        scraped: List[ScrapedMemo] = []
        for href, text in parser.links:
            full_url = urljoin(base_url, href)
            memo_id = self._memo_id_from_link(text, full_url)
            filename = self._filename_from_url(full_url, memo_id)
            published = self._extract_date(text) or self._extract_date(filename)
            scraped.append(ScrapedMemo(memo_id=memo_id, url=full_url, filename=filename, published_date=published))
        return scraped

    def download_new_memos(self, memos: Iterable[ScrapedMemo]) -> List[MemoRecord]:
        downloaded: List[MemoRecord] = []
        for memo in memos:
            if memo.memo_id in self.state.memos:
                LOGGER.debug("Skipping known memo %s", memo.memo_id)
                continue
            if self._download_breaker.is_open:
                LOGGER.error(
                    "Download circuit breaker open; skipping remaining memo downloads"
                )
                break
            record = self._download_memo(memo)
            if record:
                self.state.register_memo(record)
                downloaded.append(record)
        return downloaded

    def _download_memo(self, memo: ScrapedMemo) -> Optional[MemoRecord]:
        LOGGER.info("Downloading memo %s", memo.url)
        request = Request(memo.url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            content = execute_with_retry(
                lambda timeout: _read_binary(request, timeout),
                policy=self.config.download_retry,
                description=f"memo download {memo.memo_id}",
                logger=LOGGER,
                breaker=self._download_breaker,
            )
        except CircuitBreakerOpen:
            LOGGER.error("Download circuit breaker open; skipping memo %s", memo.memo_id)
            return None
        except Exception as exc:  # pragma: no cover - network failures
            LOGGER.error("Failed to download %s: %s", memo.url, exc)
            return None

        checksum = hashlib.sha256(content).hexdigest()
        target_path = (self.config.raw_directory / Path(memo.filename).name).resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("wb") as f:
            f.write(content)
        LOGGER.info("Saved memo to %s", target_path)

        record = MemoRecord(
            memo_id=memo.memo_id,
            url=memo.url,
            checksum=checksum,
            downloaded_at=datetime.now().astimezone().strftime(ISO_FORMAT),
            filename=target_path.name,
        )
        return record

    def _memo_id_from_link(self, text: str, url: str) -> str:
        parsed = urlparse(url)
        filename = Path(parsed.path).name
        basename = PDF_PATTERN.sub("", filename)
        normalized = _normalize_memo_id(basename)
        if date_match := DATE_PATTERN.search(text):
            year, month = date_match.groups()
            normalized = f"{year}-{int(month):02d}-adm"
        return _normalize_memo_id(normalized)

    def _filename_from_url(self, url: str, memo_id: str) -> str:
        parsed = urlparse(url)
        filename = Path(parsed.path).name
        if filename:
            return filename
        return f"{memo_id}.pdf"

    @staticmethod
    def _extract_date(text: str) -> Optional[str]:
        if date_match := DATE_PATTERN.search(text):
            year, month = date_match.groups()
            return f"{int(year):04d}-{int(month):02d}"
        return None


__all__ = ["MemoScraper", "ScrapedMemo"]


def _read_listing(request: Request, timeout: float) -> tuple[str, str]:
    with urlopen(request, timeout=timeout) as response:
        html = response.read().decode("utf-8", errors="ignore")
        base_url = response.geturl()
    return html, base_url


def _read_binary(request: Request, timeout: float) -> bytes:
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def _normalize_memo_id(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-")
    cleaned = cleaned or "memo"
    return re.sub(r"-+", "-", cleaned).lower()

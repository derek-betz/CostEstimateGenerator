from __future__ import annotations

import hashlib
from types import SimpleNamespace
from typing import List

import pytest

from memos.scraper import MemoScraper, ScrapedMemo
from memos.state import MemoRecord


class DummyResponse:
    def __init__(self, data: bytes, url: str):
        self._data = data
        self._url = url

    def read(self) -> bytes:
        return self._data

    def geturl(self) -> str:
        return self._url

    def __enter__(self) -> "DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # pragma: no cover - no special handling
        return False


@pytest.fixture
def scraper(memo_config, memo_state) -> MemoScraper:
    return MemoScraper(memo_config, memo_state)


def test_fetch_listing_parses_links(monkeypatch, scraper) -> None:
    html = """
    <html><body>
    <a href="docs/MEMO-001.pdf">Memo 2024-01</a>
    <a href="/absolute/memo-002.pdf">Memo 2024-02</a>
    </body></html>
    """

    def fake_urlopen(request, timeout=0):
        return DummyResponse(html.encode("utf-8"), "https://example.com/memos/")

    monkeypatch.setattr("memos.scraper.urlopen", fake_urlopen)

    memos = scraper.fetch_listing()
    assert [m.memo_id for m in memos] == ["2024-01-adm", "2024-02-adm"]
    assert memos[0].filename.endswith("MEMO-001.pdf")


def test_download_new_memos_saves_file(monkeypatch, scraper, memo_state) -> None:
    content = b"pdf-bytes"

    def fake_urlopen(request, timeout=0):
        return DummyResponse(content, "https://example.com/docs/memo.pdf")

    monkeypatch.setattr("memos.scraper.urlopen", fake_urlopen)

    memo = ScrapedMemo(memo_id="abc-123", url="https://example.com/docs/memo.pdf", filename="memo.pdf")
    downloaded = scraper.download_new_memos([memo])
    assert len(downloaded) == 1
    record = downloaded[0]
    saved_path = scraper.config.raw_directory / "memo.pdf"
    assert saved_path.exists()
    assert hashlib.sha256(content).hexdigest() == record.checksum
    assert memo_state.memos[memo.memo_id].filename == "memo.pdf"


def test_download_new_memos_deduplicates(monkeypatch, scraper, memo_state) -> None:
    memo_state.register_memo(
        MemoRecord(
            memo_id="abc",
            url="https://example.com/abc.pdf",
            checksum="1",
            downloaded_at="2024-01-01T00:00:00+0000",
            filename="abc.pdf",
        )
    )
    memo = ScrapedMemo(memo_id="abc", url="https://example.com/abc.pdf", filename="abc.pdf")
    downloaded = scraper.download_new_memos([memo])
    assert downloaded == []


def test_fetch_listing_retries(monkeypatch, scraper) -> None:
    attempts: List[int] = []

    def flaky_urlopen(request, timeout=0):
        attempts.append(1)
        if len(attempts) < 2:
            raise OSError("temporary failure")
        return DummyResponse(b"<a href='x.pdf'>Memo</a>", "https://example.com/memos/")

    monkeypatch.setattr("memos.scraper.urlopen", flaky_urlopen)
    monkeypatch.setattr("memos.retry.time.sleep", lambda _: None)

    scraper.config.http_retry.retries = 2
    scraper.config.http_retry.backoff_factor = 0.0

    memos = scraper.fetch_listing()
    assert len(memos) == 1
    assert len(attempts) == 2


def test_download_handles_spaces(monkeypatch, scraper, memo_state, tmp_path) -> None:
    content = b"pdf-content"
    listing = tmp_path / "listing.html"
    pdf_name = "DM 25-19 Traffic Signal Preemption_Light Trespass.pdf"
    listing.write_text(
        f'<a href="{pdf_name}">DM 25-19 Traffic Signal Preemption_Light Trespass</a>',
        encoding="utf-8",
    )
    pdf_local = tmp_path / pdf_name
    pdf_local.write_bytes(content)

    observed_urls: List[str] = []

    def fake_urlopen(request, timeout=0):
        url = getattr(request, "full_url", request)
        observed_urls.append(url)
        if str(url).lower().endswith(".html"):
            return DummyResponse(listing.read_bytes(), listing.as_uri())
        return DummyResponse(content, pdf_local.as_uri())

    monkeypatch.setattr("memos.scraper.urlopen", fake_urlopen)

    scraper.config.memo_page_url = listing.as_uri()
    scraper.config.raw_directory = tmp_path / "raw"
    scraper.config.raw_directory.mkdir(parents=True, exist_ok=True)

    memos = scraper.fetch_listing()
    assert len(memos) == 1

    downloaded = scraper.download_new_memos(memos)
    assert downloaded
    saved = scraper.config.raw_directory / memos[0].filename
    assert saved.exists()
    # ensure the request we issued has encoded spaces
    assert any("%20" in url for url in observed_urls if isinstance(url, str))


def test_download_circuit_breaker(monkeypatch, scraper) -> None:
    def failing_urlopen(request, timeout=0):
        raise OSError("fail")

    monkeypatch.setattr("memos.scraper.urlopen", failing_urlopen)
    scraper.config.download_retry.retries = 0
    scraper.config.download_retry.circuit_breaker_failures = 1
    scraper._download_breaker.threshold = 1

    memo = ScrapedMemo(memo_id="one", url="https://example.com/one.pdf", filename="one.pdf")
    assert scraper.download_new_memos([memo]) == []
    assert scraper._download_breaker.is_open
    memo2 = ScrapedMemo(memo_id="two", url="https://example.com/two.pdf", filename="two.pdf")
    assert scraper.download_new_memos([memo2]) == []

# Memo Extraction Test Baseline

## Configuration Sources
- `references/memos/config.json` - default runtime configuration; includes storage paths, SMTP/IMAP stubs, and retry settings.
- Environment overrides:
  - HTTP retry: `MEMO_HTTP_TIMEOUT_SECONDS`, `MEMO_HTTP_RETRIES`, `MEMO_HTTP_BACKOFF_FACTOR`
  - Download retry: `MEMO_DOWNLOAD_TIMEOUT_SECONDS`, `MEMO_DOWNLOAD_RETRIES`, `MEMO_DOWNLOAD_BACKOFF_FACTOR`
  - Notification recipients: `SMTP_SENDER`, `SMTP_RECIPIENTS`, `SMTP_NOTIFY_ON_FAILURE`
  - Mailbox credentials: `IMAP_HOST`, `IMAP_USERNAME`, `IMAP_PASSWORD`, `IMAP_PORT`
- Schema: `references/memos/schema/processed.schema.json` — validates structured summaries (`metadata`, `highlights`, `snippets`).

## Retry & Circuit Breaker Defaults
| Operation | Timeout (s) | Retries | Backoff Factor | Circuit Breaker Failures |
|-----------|-------------|---------|----------------|--------------------------|
| Listing fetch (`config.http_retry`) | 30 | 0 | 0.0 | 3 |
| PDF download (`config.download_retry`) | 60 | 0 | 0.0 | 3 |
| SMTP (if enabled) | 30 | 0 | 0.0 | 3 |
| IMAP approval (if enabled) | 30 | 0 | 0.0 | 3 |

## Storage Layout
- Raw downloads: `references/memos/raw/*.pdf`
- Structured summaries: `references/memos/processed/*.json`
- Markdown digests: `references/memos/digests/*.md`
- Workflow state: `references/memos/state.json`
- Overlay mappings: `references/memos/mappings/` (empty stub to populate per environment)

## Baseline Test Scenarios
- **Happy path** - well-formed memo with pay item, spec section, and effective date; expect JSON+MD outputs and notification when enabled.
- **Metadata rich** - memo containing replacement/obsolete codes, currency values, and keywords to exercise metadata extraction and snippet collation.
- **Malformed/guarded** - memo missing schema fields or exceeding pay-item limit to verify guarded detection and validation failure handling.
- **Fallback pricing overlay** - simulate missing overlay file to confirm workflow logs guarded fallback and continues (handled via tests that monkeypatch overlay loader).
- **Circuit breaker** - force consecutive fetch/download failures to ensure breaker trips after configured threshold and prevents additional network calls.

## Observability Expectations
- Logging emits workflow summary with fetched/downloaded/parsed counts and notification status.
- Parser exposes `last_failed_count` for metrics aggregation.
- Retry wrapper logs warning for each retry and breaker-open error at INFO/ERROR level.

## Test Data Checklist
- Synthetic PDFs generated via `tests/memos/conftest::pdf_factory`.
- Schema copied into ephemeral temp dirs before parsing tests.
- Stub SMTP/IMAP credentials left blank for sandbox runs; populate via environment for live integration.

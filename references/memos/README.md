# INDOT Active Design Memos Archive

This directory holds automatically retrieved INDOT Active Design Memos and
associated structured summaries for use by the Cost Estimate Generator.

```
references/memos/
  raw/        # Original PDF downloads (immutable once archived)
  processed/  # Machine-readable JSON summaries of each memo
  digests/    # Human-readable Markdown digests assembled from parsed data
  state.json  # Persistent state describing known memos/checksums
  config.json # Runtime configuration (memo URL, schedule metadata, email settings)
```

The automation scripts under `scripts/memos/` are responsible for fetching new
memos, parsing them, and preparing notification artifacts. Summaries are only
committed to the repository after explicit approval from the designated
reviewer.

## Validate digests vs PDFs

Use `scripts/memos/validate_and_correct.py` to cross-check Markdown digests and
JSON summaries against the source PDF text. By default it performs a
deterministic re-parse and reports differences. With `--use-ai`, it also asks
an AI model to flag potential omissions and proposes highlight fixes, which are
locally verified against the PDF text before applying to avoid hallucinations.

Examples:

```
poetry run python scripts/memos/validate_and_correct.py --verbose
poetry run python scripts/memos/validate_and_correct.py --use-ai --apply --limit 5
```

The validator reads `OPENAI_API_KEY` from the environment or `API_KEY/API_KEY.txt`.

## Configuration overview

`config.json` (or the YAML equivalent) controls the workflow.  The following
sections supplement the existing SMTP/IMAP credentials:

- `http` and `download` – retry/backoff policy for the memo listing request and
  individual PDF downloads. Fields include `timeout_seconds`, `retries`,
  `backoff_factor`, and `circuit_breaker_failures`. Environment overrides:
  `MEMO_HTTP_TIMEOUT`, `MEMO_HTTP_RETRIES`, `MEMO_HTTP_BACKOFF`,
  `MEMO_DOWNLOAD_TIMEOUT`, `MEMO_DOWNLOAD_RETRIES`, `MEMO_DOWNLOAD_BACKOFF`.
- `notification.enabled_on_failure` – send a short `[ALERT] Memo workflow failed`
  email when the GitHub Action fails. Requires `notification.enabled` or a
  `force=True` send and a valid SMTP block. Override with
  `SMTP_NOTIFY_ON_FAILURE=1`.
- `notification.smtp.retry` and `approval.mailbox.retry` – granular retry
  configuration for SMTP/IMAP (same fields as HTTP). Environment overrides:
  `SMTP_TIMEOUT`, `SMTP_RETRIES`, `SMTP_BACKOFF`, `IMAP_TIMEOUT`, `IMAP_RETRIES`,
  `IMAP_BACKOFF`.
- `patterns` – customise the parser’s regular expressions and keyword list
  (`pay_item_regex`, `spec_section_regex`, `dollar_regex`, `keywords`) along
  with `pay_item_limit` and `pay_item_frequency_guard` to control false
  positives.

Note: Pay items are expected in strict INDOT format `xxx-xxxxx` or `xxx-xxxxxx`
(three digits, hyphen, then five or six digits). The default `pay_item_regex`
enforces this to avoid false positives from phone numbers or dates.

Environment variables continue to supply credentials when preferred:

- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_SENDER`,
  `SMTP_RECIPIENTS`
- `IMAP_HOST`, `IMAP_PORT`, `IMAP_USERNAME`, `IMAP_PASSWORD`, `IMAP_FOLDER`

## Processed memo schema

Structured summaries are validated against
`schema/processed.schema.json`.  Update the schema when adding fields and ensure
tests cover the change.  Validation failures mark the memo in `state.json` with
an error rather than aborting the run, allowing partial progress when a single
PDF causes trouble.

## Design memo mappings

The static replacement-to-obsolete mapping in
`src/costest/design_memos.py` can be augmented with
`mappings/design_memo_mappings.csv`.  Each row should provide
`memo_id,effective_date,replacement_code,obsolete_code`.  The loader groups
rows by replacement code so multiple obsolete items may be listed.  When CSV
and static mappings conflict, the static entry wins to preserve historical
behaviour.

## Troubleshooting

- **Repeated network failures:** increase `retries`/`backoff_factor` in the
  relevant section (`http`, `download`, `notification.smtp.retry`, or
  `approval.mailbox.retry`). Circuit breakers (`circuit_breaker_failures`) stop
  hammering endpoints after consecutive errors and log that the category was
  skipped.
- **Schema validation errors:** inspect the JSON written to `processed/` and the
  corresponding error recorded in `state.json`. Adjust the schema or parser and
  rerun once resolved.
- **Failure alerts not sent:** ensure `notification.enabled_on_failure` is true
  and the SMTP block is valid. Alerts respect the same retry policy as the
  normal success notifications.

# Memo Automation Scripts

This folder contains helper entry points for automating the INDOT Active Design
Memos ingestion pipeline.

- `run_pipeline.py` — Scrape the memo page, download new PDFs, parse highlights,
  and optionally send notification emails.
- `prepare_approved.py` — Poll the configured approval mechanism to confirm
  which memos are approved and update the consolidated index for commit.

Both scripts are designed to run inside the project virtual environment:

```
poetry run python scripts/memos/run_pipeline.py --verbose
poetry run python scripts/memos/prepare_approved.py --verbose
```

Configuration is sourced from `references/memos/config.json`. Update that file
with valid SMTP/IMAP credentials before enabling notifications or approvals.
To activate AI-assisted memo reviews, keep the `ai.enabled` flag set to `true`
and ensure the OpenAI API key can be read from the configured path (defaults to
`C:\AI\CostEstimateGenerator\API_KEY\API_KEY.txt`) or via the
`OPENAI_API_KEY` environment variable.

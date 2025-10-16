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

Populate `config.json` with SMTP/IMAP credentials or provide them via
environment variables when running the scripts or GitHub Action:

- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_SENDER`,
  `SMTP_RECIPIENTS`
- `IMAP_HOST`, `IMAP_PORT`, `IMAP_USERNAME`, `IMAP_PASSWORD`, `IMAP_FOLDER`

## AI-assisted review

An optional OpenAI-assisted reviewer expands each digest with reasoning-based
insights. Enable it via the `ai` section in `config.json`. By default the
automation reads the API key from `C:\AI\CostEstimateGenerator\API_KEY\API_KEY.txt`;
adjust the path if the key is stored elsewhere or set the `OPENAI_API_KEY`
environment variable. When enabled, AI feedback is written next to the
human-readable digest and the
summary JSON gains an `ai_analysis` block that downstream tooling can inspect.

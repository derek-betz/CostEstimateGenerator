# Cost Estimate Generator

The Cost Estimate Generator ingests historical pay-item pricing data, computes
summary statistics, and updates estimate workbooks and audit CSV files in
place. The project ships with synthetic sample data that demonstrate the
expected file layout and allow the pipeline to be exercised end-to-end without
external services.

## Requirements

### Software Requirements
- **Python**: 3.9 or higher
- **pip**: Python package installer (typically included with Python)

### Python Package Dependencies
The following packages are installed via `requirements.txt` or `pyproject.toml`:
- `numpy==1.26.4` - Numerical computing
- `pandas==1.5.3` - Data analysis and manipulation
- `openpyxl==3.1.2` - Excel file reading/writing
- `python-dotenv==1.0.0` - Environment variable management
- `xlrd==2.0.1` - Legacy Excel file reading
- `openai>=1.0.0,<2.0.0` - AI assistance (optional, can be disabled)
- `reportlab>=4.0.0,<5.0.0` - PDF generation
- `pypdf>=3.1.0,<5.0.0` - PDF parsing/manipulation
- `jsonschema>=4.19.0,<5.0.0` - Validation of memo summary payloads

### Optional Features
- **OpenAI API**: To enable AI-assisted item mapping, you need:
- An OpenAI API key (set via `OPENAI_API_KEY`, or stored in `API_KEY/API_KEY.txt`;
  alternatively point to a specific file with `OPENAI_API_KEY_FILE`)
  - Can be disabled with `DISABLE_OPENAI=1` environment variable or `--disable-ai` flag

## Features

- Reads historical price data from Excel workbooks (sheet-per-item) and from
  directories of CSV files, aggregating every matching source for a pay item.
- Computes `DATA_POINTS_USED`, `MEAN_UNIT_PRICE`, `STD_DEV`, `COEF_VAR`, and a
  confidence score per pay item using the formula
  `confidence = (1 - exp(-n/30)) * (1 / (1 + cv))`.
- Updates `Estimate_Draft.xlsx` by inserting a `CONFIDENCE` column immediately
  after `DATA_POINTS_USED` within the `Estimate` sheet.
- Updates `Estimate_Audit.csv` by inserting `STD_DEV` and `COEF_VAR` columns
  after `DATA_POINTS_USED` and populating them for every row.
- Produces a debug mapping report at `outputs/payitem_mapping_debug.csv` listing
  any DM 23-21 remappings (`source_item`, `mapped_item`, `mapping_rule`,
  `adder_applied`, `evidence`).
- Supports `--dry-run` mode and optional AI assistance that can be disabled
  via CLI flags or the `DISABLE_OPENAI=1` environment variable.
- Automates retrieval of INDOT Active Design Memos, producing structured
  summaries under `references/memos/processed/` and Markdown digests in
  `references/memos/digests/` to highlight pay-item updates for review.
- Validates processed memo JSON against `references/memos/schema/processed.schema.json`
  so downstream tooling receives consistent metadata.
- Supports optional failure alerts when the memo ingest CI workflow fails
  (`notification.enabled_on_failure`) and standardises retry/backoff behaviour
  for HTTP, SMTP, and IMAP integrations.
- Allows design memo rollup mappings to be extended at runtime via
  `references/memos/mappings/design_memo_mappings.csv` without modifying the
  bundled defaults.

## Fallback pricing

Items that lack usable bid history now receive conservative prices from
non-geometry fallbacks:

- **Unit Price Summary (CY2024)** – if a weighted average exists with at least
  three supporting contracts, the pipeline adjusts the summary price for
  recency (STATE 12M vs. 24/36M) and region (DIST vs. STATE) before clamping it
  to the published low/high range.
- **Design memo rollups** – when summary support is thin or missing, the
  replacement code can inherit data from its obsolete counterparts.  Static
  mappings live in `src/costest/design_memos.py` (e.g., DM 25-10 pooling
  `401-10258/401-10259` into `401-11526`) and can be supplemented with
  additional rows in `references/memos/mappings/design_memo_mappings.csv`
  (columns: `memo_id,effective_date,replacement_code,obsolete_code`).  Static
  entries win on conflicts to preserve legacy behaviour.

Each fallback sets `SOURCE`, `DATA_POINTS_USED`, and detailed `NOTES` so the
Excel and CSV outputs clearly explain how the estimate was derived.  The
existing geometry-based alternate seek continues to operate unchanged and only
activates when both category pricing and the new fallbacks provide no data.

## Project inputs

Place the project-level spreadsheets exported from the front end in
`data_sample/` (or pass explicit paths via `--project-quantities` and
`--project-attributes`):

- `*_project_quantities.xlsx` lists the pay items included in the job.
- `project_attributes.xlsx` *(optional)* previously stored the expected contract
  cost and project district. The graphical launcher now collects these values
  via dedicated input fields, but the workbook remains supported for CLI-driven
  workflows.
- `BidTabsData/` holds historical bid tab exports (legacy `.xls` files) that
  supply the price history used when computing statistics.

When present, the CLI automatically loads these files and attaches the metadata
to the mapping report.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
# PowerShell: .\.venv\Scripts\Activate.ps1
pip install -e .[dev]
```
Use `pip install -e .` if you only need the runtime dependencies or `pip install -r requirements.txt` to install the pinned production set without development tooling.
Run `costest --help` to see the full command-line interface.

### Graphical drag-and-drop interface

For a lightweight desktop launcher run:

```bash
costest-gui
```

An application window opens where you can drag and drop the
``*_project_quantities.xlsx`` workbook.  The estimator pipeline executes using
that workbook and writes the results to the standard output locations.  For
native drag-and-drop support install the optional ``tkinterdnd2`` package
(``pip install tkinterdnd2``); without it the window still offers a "Browse"
button for manual file selection.

The launcher prompts for the Expected Total Contract Cost (currency field with a
leading ``$``) and the Project District (drop-down listing the six INDOT
districts). These entries replace the previous requirement to populate
``project_attributes.xlsx`` when starting a run from the GUI.

Generate fresh sample output files from the text templates:

```bash
python scripts/prepare_sample_outputs.py
```

The script copies the CSV audit sample and materialises Excel workbooks from
`data_sample/Estimate_Draft_template.csv` and
`data_sample/payitems_workbook.json` into the `outputs/` directory. The sample
project spreadsheets `data_sample/2300946_project_quantities.xlsx` and
`data_sample/project_attributes.xlsx` mirror the front-end payload and remain
available for CLI examples, though the GUI now captures the same metadata
interactively.
With those files in place, run the pipeline against the samples:

```bash
costest \
  --payitems-workbook outputs/PayItems_Audit.xlsx \
  --estimate-audit-csv outputs/Estimate_Audit.csv \
  --estimate-xlsx outputs/Estimate_Draft.xlsx
```

Override any input via the matching CLI flags (for example,
`--project-quantities data_sample/2300946_project_quantities.xlsx`).
When run without explicit paths the CLI looks for `outputs/PayItems_Audit.xlsx`
and falls back to the bundled sample workbook or to a `data_in/` directory if
present. Supply `--mapping-debug-csv` to write the mapping report to a custom
location.

`DISABLE_OPENAI=1` is respected automatically; set it (or use the
`--disable-ai` flag) when running offline. If AI assistance is desired, provide
an API key via the `OPENAI_API_KEY` environment variable or by storing it in
`API_KEY/API_KEY.txt` and omitting the disable flag.

A convenience wrapper is available:

```bash
python scripts/run_pipeline.py --help
```

## Handling HMA Pay Item Transition (DM 23-21)

INDOT Design Memo 23-21 introduces new HMA pay item numbers that supersede the
legacy PG binder-based codes. The estimator supports this transition by:

- Loading the memo crosswalk from `data_reference/hma_crosswalk_dm23_21.csv`
  (excludes SMA entries marked as deleted). Each row records the legacy pay
  item, its new MSCR counterpart, the mix course (Surface/Intermediate/Base),
  ESAL category, and binder class.
- Remapping historical BidTabs records and project quantities to the new item
  numbers whenever `--apply-dm23-21` (or `APPLY_DM23_21=1`) is provided. SMA
  items flagged as deleted are skipped automatically and listed in the logs.
- Annotating estimate rows with `MappedFromOldItem`, mix metadata, and a
  transitional adder flag. The mapping debug report (`payitem_mapping_debug.csv`)
  captures `source_item`, `mapped_item`, `mapping_rule`, `adder_applied`, and an
  `evidence` column fixed to "DM 23-21" for traceability.
- Applying transitional adders of $3.00/ton (Surface), $2.50/ton
  (Intermediate), or $2.00/ton (Base) whenever DM 23-21 logic is enabled but the
  new item lacks sufficient history. Adders are automatically removed once the
  minimum sample target is satisfied.

To enable the new behaviour in CLI runs, pass `--apply-dm23-21` (or export the
environment variable `APPLY_DM23_21=1`). The graphical launcher respects the
same environment variable.

## Testing

Run the automated test suite with:

```bash
python -m pytest -q
```

Continuous integration runs the same command on every push via GitHub Actions.

## Project layout

```
CostEstimateGenerator/
+-- src/costest/                # Library code
+-- data_sample/                # Synthetic sample inputs
+-- outputs/                    # Target directory for generated outputs
+-- scripts/run_pipeline.py     # CLI wrapper
+-- tests/                      # Pytest-based unit and integration tests
+-- requirements.txt            # Reproducible dependency pins
+-- pyproject.toml              # Packaging metadata
```

The project is designed to be idempotent: running the pipeline multiple times
with the same inputs produces consistent outputs.

## Pricing fallback tiers and configuration

When estimating unit prices, the pipeline uses the following tiers in order:

1. Historical category mix (BidTabs):
   - District and statewide windows (12/24/36 months) aggregated using the configured method.
2. Design memo rollup:
   - Uses officially replaced/rolled-up item codes to form a pooled set and applies the same adjustments.
3. Unit Price Summary (UPS):
  - Falls back to the statewide weighted average for the specific item when sufficient UPS contracts exist.
4. NO_DATA:
   - If none of the above tiers apply, the item remains with a $0.00 placeholder and a review note.

Notes and metrics:
- Fallback tiers annotate SOURCE (e.g., `DESIGN_MEMO_ROLLUP`, `UNIT_PRICE_SUMMARY`) and add details in NOTES.
- Confidence is computed in the exports to help triage low-data items.
- Quantity window and sigma trimming thresholds are configurable via
  `MEMO_ROLLUP_QUANTITY_LOWER`, `MEMO_ROLLUP_QUANTITY_UPPER`, and
  `MEMO_ROLLUP_SIGMA_THRESHOLD` environment variables (defaults remain 0.5/1.5
  and ±2σ respectively).

Configuration toggles:
- Alternate-seek toggle (geometry-based backfill):
  - Environment: `DISABLE_ALT_SEEK=1` to disable.
  - GUI: “Enable alternate seek backfill” checkbox.
  

Dashboard / summary:
- The run summary now includes a count of items priced via alternates.

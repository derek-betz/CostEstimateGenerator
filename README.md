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
- `PyPDF2==3.0.1` - PDF manipulation
- `pytest==7.4.4` - Testing framework

### Optional Features
- **OpenAI API**: To enable AI-assisted item mapping, you need:
  - An OpenAI API key (set via `OPENAI_API_KEY` environment variable or in `API_KEY/API_KEY.txt`)
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
- Produces a debug mapping report at `outputs/payitem_mapping_debug.csv` showing
  how item codes were matched to historical sources.
 - Supports `--dry-run` mode and optional AI assistance that can be disabled
   via CLI flags or the `DISABLE_OPENAI=1` environment variable.

## Project inputs

Place the project-level spreadsheets exported from the front end in
`data_sample/` (or pass explicit paths via `--project-quantities` and
`--project-attributes`):

- `*_project_quantities.xlsx` lists the pay items included in the job.
- `project_attributes.xlsx` contains the anticipated contract cost and district
  location used to enrich the debug output.
- `BidTabsData/` holds historical bid tab exports (legacy `.xls` files) that
  supply the price history used when computing statistics.

When present, the CLI automatically loads these files and attaches the metadata
to the mapping report.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate  # PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -e .
```

Generate fresh sample output files from the text templates:

```bash
python scripts/prepare_sample_outputs.py
```

The script copies the CSV audit sample and materialises Excel workbooks from
`data_sample/Estimate_Draft_template.csv` and
`data_sample/payitems_workbook.json` into the `outputs/` directory. The sample
project spreadsheets `data_sample/2300946_project_quantities.xlsx` and
`data_sample/project_attributes.xlsx` mirror the front-end payload.
With those files in place, run the pipeline against the samples:

```bash
python -m costest.cli \
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

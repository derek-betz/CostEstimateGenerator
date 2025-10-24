from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pandas as pd

from costest.cli import run
from costest.config import load_cli_config
from costest.sample_data import DATA_SAMPLE_DIR, create_estimate_workbook_from_template, create_payitems_workbook_from_template


def test_contract_percents_externalized(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()

    # Seed outputs
    (outputs / "Estimate_Audit.csv").write_text((DATA_SAMPLE_DIR / "Estimate_Audit.csv").read_text(encoding="utf-8"), encoding="utf-8")
    create_estimate_workbook_from_template(DATA_SAMPLE_DIR / "Estimate_Draft_template.csv", outputs / "Estimate_Draft.xlsx")
    payitems_workbook = tmp_path / "PayItems_Audit.xlsx"
    create_payitems_workbook_from_template(DATA_SAMPLE_DIR / "payitems_workbook.json", payitems_workbook)

    args = SimpleNamespace(
        input_payitems=DATA_SAMPLE_DIR / "payitems",
        estimate_audit_csv=outputs / "Estimate_Audit.csv",
        estimate_xlsx=outputs / "Estimate_Draft.xlsx",
        payitems_workbook=payitems_workbook,
        mapping_debug_csv=outputs / "payitem_mapping_debug.csv",
        disable_ai=True,
        api_key_file=None,
        dry_run=False,
        log_level="INFO",
    )
    cfg = load_cli_config(args)

    rc = run(cfg)
    assert rc == 0

    # Verify run metadata was written and contains spec edition
    meta_path = outputs / "run_metadata_table.json"
    assert meta_path.exists()
    meta = pd.read_json(meta_path)
    assert "spec_edition" in meta.columns

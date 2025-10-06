"""Generate sample output files from the shipped templates."""
from __future__ import annotations

import shutil
from pathlib import Path

from costest.sample_data import (
    DATA_SAMPLE_DIR,
    create_estimate_workbook_from_template,
    create_payitems_workbook_from_template,
)


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    outputs = project_root / "outputs"
    outputs.mkdir(parents=True, exist_ok=True)

    estimate_template = DATA_SAMPLE_DIR / "Estimate_Draft_template.csv"
    estimate_dest = outputs / "Estimate_Draft.xlsx"
    create_estimate_workbook_from_template(estimate_template, estimate_dest)

    payitems_template = DATA_SAMPLE_DIR / "payitems_workbook.json"
    payitems_dest = outputs / "PayItems_Audit.xlsx"
    create_payitems_workbook_from_template(payitems_template, payitems_dest)

    audit_src = DATA_SAMPLE_DIR / "Estimate_Audit.csv"
    audit_dest = outputs / "Estimate_Audit.csv"
    shutil.copy(audit_src, audit_dest)

    print(f"Wrote sample Estimate_Audit.csv to {audit_dest}")
    print(f"Wrote sample Estimate_Draft.xlsx to {estimate_dest}")
    print(f"Wrote sample PayItems_Audit.xlsx to {payitems_dest}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import importlib
from pathlib import Path

import costest.design_memos as design_memos


def test_load_additional_mappings(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "design_memo_mappings.csv"
    csv_path.write_text(
        """memo_id,effective_date,replacement_code,obsolete_code
DM-1,2024-05-01,401-99999,401-88888
DM-1,2024-05-01,401-99999,401-77777
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("DESIGN_MEMO_MAPPINGS_FILE", str(csv_path))
    importlib.reload(design_memos)

    mapping = design_memos.get_obsolete_mapping("401-99999")
    assert mapping is not None
    assert mapping["memo_id"] == "DM-1"
    assert set(mapping["obsolete_codes"]) == {"401-88888", "401-77777"}

    # Static mapping should take precedence even if CSV attempts override
    csv_path.write_text(
        """memo_id,effective_date,replacement_code,obsolete_code
25-10,2025-05-01,401-11526,401-00000
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("DESIGN_MEMO_MAPPINGS_FILE", str(csv_path))
    importlib.reload(design_memos)
    mapping_static = design_memos.get_obsolete_mapping("401-11526")
    assert mapping_static is not None
    assert "40100000" not in mapping_static["obsolete_codes"]

    # Clean up environment for future imports
    monkeypatch.delenv("DESIGN_MEMO_MAPPINGS_FILE", raising=False)
    importlib.reload(design_memos)

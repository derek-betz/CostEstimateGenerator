from __future__ import annotations

import pandas as pd

from costest.bidtabs_io import load_quantities


def test_load_quantities_supports_alt_project_headers(tmp_path):
    df = pd.DataFrame(
        {
            "Pay Item Number": ["110-00001", "20304050"],
            "Pay Item Name": ["Clearing Right of Way", "Excavation"],
            "Unit": ["LS", "CY"],
            "Quantity": [1, 250.5],
        }
    )
    path = tmp_path / "alt_headers.xlsx"
    df.to_excel(path, index=False)

    result = load_quantities(path)

    assert list(result.columns) == ["ITEM_CODE", "DESCRIPTION", "UNIT", "QUANTITY"]
    assert result["ITEM_CODE"].tolist() == ["110-00001", "203-04050"]
    assert result["DESCRIPTION"].tolist() == ["Clearing Right of Way", "Excavation"]
    assert result["UNIT"].tolist() == ["LS", "CY"]
    assert result["QUANTITY"].tolist() == [1, 250.5]

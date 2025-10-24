from costest.cli import _apply_geometry_summary_price, _round_unit_price


def _make_row() -> dict:
    return {
        "ITEM_CODE": "714-12345",
        "UNIT_PRICE_EST": 0.0,
        "NOTES": "NO DATA IN ANY CATEGORY; REVIEW.",
        "DATA_POINTS_USED": 0,
        "ALTERNATE_USED": False,
        "SOURCE": "NO_DATA",
    }


def test_apply_geometry_summary_price_applies_with_sufficient_contracts():
    row = _make_row()
    summary_lookup = {
        "714-12345": {
            "weighted_average": 250.0,
            "contracts": 4,
        }
    }

    applied, data_points, unit_price = _apply_geometry_summary_price(
        row,
        row["ITEM_CODE"],
        object(),
        row["DATA_POINTS_USED"],
        summary_lookup,
    )

    assert applied is True
    assert data_points == 4
    assert unit_price == _round_unit_price(250.0)
    assert row["SOURCE"] == "UNIT_PRICE_SUMMARY"
    assert row["DATA_POINTS_USED"] == 4
    assert row["ALTERNATE_USED"] is False
    assert "Unit Price Summary weighted average used (4 contracts)." in row["NOTES"]
    assert "NO DATA" not in row["NOTES"]


def test_apply_geometry_summary_price_requires_minimum_contracts():
    row = _make_row()
    summary_lookup = {
        "714-12345": {
            "weighted_average": 250.0,
            "contracts": 1,
        }
    }

    applied, data_points, unit_price = _apply_geometry_summary_price(
        row,
        row["ITEM_CODE"],
        object(),
        row["DATA_POINTS_USED"],
        summary_lookup,
    )

    assert applied is False
    assert data_points == 0
    assert unit_price == 0.0
    assert row["SOURCE"] == "NO_DATA"
    assert row["DATA_POINTS_USED"] == 0
    assert row["NOTES"] == "NO DATA IN ANY CATEGORY; REVIEW."

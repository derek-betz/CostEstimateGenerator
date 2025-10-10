from pathlib import Path

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")  # type: ignore[attr-defined]

from costest.visuals import emit_visualizations


def test_emit_visualizations_creates_charts(tmp_path):
    df = pd.DataFrame(
        {
            "ITEM_CODE": ["101-00001"],
            "DATA_POINTS_USED": [8],
            "UNIT_PRICE_EST": [42.5],
            "EXTENDED": [425.0],
        }
    )

    detail = pd.DataFrame(
        {
            "ITEM_CODE": ["101-00001"] * 5,
            "UNIT_PRICE": [40, 42, 44, 41, 43],
            "CATEGORY": ["STATE_12M", "STATE_12M", "DIST_12M", "DIST_12M", "STATE_12M"],
            "USED_FOR_PRICING": [True, True, True, True, True],
            "LETTING_DATE": pd.date_range("2022-01-01", periods=5, freq="MS"),
            "REGION": [1, 1, 1, 1, 1],
        }
    )

    payitem_details = {"101-00001": detail}

    output_dir = (tmp_path / "visuals").resolve()
    result = emit_visualizations(
        df,
        payitem_details,
        output_dir,
        top_n_items=5,
        format="png",
        bundle_pdf=False,
    )

    assert result["charts"], "Expected at least one chart path to be returned"
    chart_names = {Path(path).name for path in result["charts"]}
    assert "overall_unit_price_hist.png" in chart_names
    for chart_path in result["charts"]:
        chart_file = Path(chart_path)
        assert chart_file.exists()
        assert chart_file.parent == output_dir
    assert result["pdf"] is None

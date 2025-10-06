from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from costest.io import extract_price_series


def test_extract_price_series_prefers_explicit_price_columns():
    df = pd.DataFrame(
        {
            "Unit Price": [10, 12, None],
            "DIST_AVG": [9, 11, 10],
        }
    )
    result = extract_price_series(df)
    assert result.used_fallback is False
    assert result.columns_used == ["Unit Price"]
    assert result.values.tolist() == [10.0, 12.0]


def test_extract_price_series_uses_fallback_when_needed():
    df = pd.DataFrame(
        {
            "Unit Price": [None, None],
            "DIST_PRICE": [8, 9],
        }
    )
    result = extract_price_series(df)
    assert result.used_fallback is True
    assert result.columns_used == ["DIST_PRICE"]
    assert result.values.tolist() == [8.0, 9.0]


def test_extract_price_series_handles_empty_fallback():
    df = pd.DataFrame(
        {
            "DIST_A": ["bad", None],
            "STATE_B": [None, ""],
        }
    )
    result = extract_price_series(df)
    assert result.used_fallback is True
    assert result.values.tolist() == []

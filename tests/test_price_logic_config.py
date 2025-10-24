from __future__ import annotations

import importlib
import os

import numpy as np
import pandas as pd

import costest.price_logic as price_logic


def test_rollup_defaults_preserved():
    df = pd.DataFrame(
        {
            "ITEM_CODE": ["A", "A", "A"],
            "UNIT_PRICE": [10, 12, 11],
            "QUANTITY": [100, 140, 160],
        }
    )
    pool = price_logic.prepare_memo_rollup_pool(df, ["A"], target_quantity=100)
    assert np.isclose(pool["UNIT_PRICE"].mean(), 11.0)


def test_rollup_env_override(monkeypatch):
    monkeypatch.setenv("MEMO_ROLLUP_QUANTITY_LOWER", "0.9")
    monkeypatch.setenv("MEMO_ROLLUP_QUANTITY_UPPER", "1.1")
    monkeypatch.setenv("MEMO_ROLLUP_SIGMA_THRESHOLD", "1.0")
    importlib.reload(price_logic)

    df = pd.DataFrame(
        {
            "ITEM_CODE": ["B", "B", "B"],
            "UNIT_PRICE": [10, 20, 1000],
            "QUANTITY": [90, 105, 400],
        }
    )
    pool = price_logic.prepare_memo_rollup_pool(df, ["B"], target_quantity=100)
    assert len(pool) == 2  # quantity filter removes third row
    assert pool["UNIT_PRICE"].max() < 1000  # sigma filter removed outlier

    monkeypatch.delenv("MEMO_ROLLUP_QUANTITY_LOWER", raising=False)
    monkeypatch.delenv("MEMO_ROLLUP_QUANTITY_UPPER", raising=False)
    monkeypatch.delenv("MEMO_ROLLUP_SIGMA_THRESHOLD", raising=False)
    importlib.reload(price_logic)


def test_quantity_filter_expands_when_pool_sparse():
    df = pd.DataFrame(
        {
            "ITEM_CODE": ["Q"] * 18,
            "UNIT_PRICE": np.linspace(10, 27, 18),
            "QUANTITY": [60.0] * 9 + [190.0] * 5 + [20.0] * 4,
        }
    )
    price, source, cat_data, detail_map, used_categories, combined_detail = price_logic.category_breakdown(
        df, "Q", project_region=None, include_details=True, target_quantity=100.0
    )
    assert cat_data["TOTAL_USED_COUNT"] == 14
    assert cat_data["QUANTITY_FILTER_BASE_COUNT"] == 9.0
    assert cat_data["QUANTITY_FILTER_WAS_EXPANDED"] is True
    assert cat_data["QUANTITY_FILTER_LOWER_MULTIPLIER"] == 0.5
    assert cat_data["QUANTITY_FILTER_UPPER_MULTIPLIER"] == 2.0
    assert len(combined_detail) == 14
    assert (combined_detail["QUANTITY"] >= 50.0).all()


def test_quantity_filter_stays_tight_when_enough_points():
    df = pd.DataFrame(
        {
            "ITEM_CODE": ["R"] * 15,
            "UNIT_PRICE": np.linspace(15, 29, 15),
            "QUANTITY": [70.0] * 12 + [220.0] * 3,
        }
    )
    price, source, cat_data = price_logic.category_breakdown(
        df, "R", project_region=None, include_details=False, target_quantity=100.0
    )
    assert cat_data["TOTAL_USED_COUNT"] == 12
    assert cat_data["QUANTITY_FILTER_BASE_COUNT"] == 12.0
    assert cat_data["QUANTITY_FILTER_WAS_EXPANDED"] is False
    assert cat_data["QUANTITY_FILTER_LOWER_MULTIPLIER"] == 0.5
    assert cat_data["QUANTITY_FILTER_UPPER_MULTIPLIER"] == 1.5

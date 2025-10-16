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

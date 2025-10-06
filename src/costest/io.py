from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

import pandas as pd


@dataclass(frozen=True)
class PriceSeries:
    values: pd.Series
    columns_used: List[str]
    used_fallback: bool


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def extract_price_series(df: pd.DataFrame) -> PriceSeries:
    """Extract a numeric price series from a dataframe.

    Preference order:
    1) Explicit "Unit Price" column when it has numeric values
    2) Any single fallback column containing "PRICE" or starting with DIST_/STATE_ with numeric values
    Returns an empty series when nothing usable is found.
    """
    # 1) Prefer explicit Unit Price
    if "Unit Price" in df.columns:
        s = _coerce_numeric(df["Unit Price"]).dropna()
        if not s.empty:
            return PriceSeries(values=s.reset_index(drop=True), columns_used=["Unit Price"], used_fallback=False)

    # 2) Fallback scan
    fallback_cols: List[str] = []
    for col in df.columns:
        name = str(col)
        if name == "Unit Price":
            continue
        upper = name.upper()
        if "PRICE" in upper or upper.startswith("DIST_") or upper.startswith("STATE_"):
            fallback_cols.append(name)

    for col in fallback_cols:
        s = _coerce_numeric(df[col]).dropna()
        if not s.empty:
            return PriceSeries(values=s.reset_index(drop=True), columns_used=[col], used_fallback=True)

    # No luck: return empty
    return PriceSeries(values=pd.Series(dtype=float), columns_used=[], used_fallback=True)

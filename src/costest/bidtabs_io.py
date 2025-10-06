"""
BidTabs / Quantities IO helpers
- Reads all CSV/XLS/XLSX files from data_in\bidtabs_raw
- Normalizes headers (incl. aliasing BID_DATE -> LETTING_DATE)
- Ensures a numeric REGION column (maps from DISTRICT when needed)
- Loads project quantities (supports PAY ITEM header)
- Finds the correct quantities file via a glob pattern (7-digit Des prefix)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Union

import pandas as pd

# ------------ Header normalization map ------------
# Add common variants here so we can rename them to our internal names.
HEADER_MAP = {
    "item_code": ["ITEM_CODE", "ITEM NO", "ITEMID", "ITEM ID", "PAY ITEM", "ITEM"],
    "desc":      ["ITEM_DESCRIPTION", "DESCRIPTION", "ITEM DESC", "ITEM DESCRIPTION"],
    "unit":      ["UNIT", "UOM"],
    "qty":       ["QUANTITY", "QTY"],
    "price":     ["UNIT_PRICE", "UNIT PRICE", "PRICE"],
    # IMPORTANT: Support both LETTING_* and BID_* names for date:
    "letting":   ["LETTING_DATE", "LETTING", "LETTING DATE", "BID_DATE", "BID DATE"],
    "county":    ["COUNTY"],
    "district":  ["DISTRICT", "DIST"],
    "region":    ["REGION"],
    "bidder":    ["BIDDER", "CONTRACTOR", "BIDDER_NAME"],
    "weight":    ["WGT", "WEIGHT", "WEIGHTED", "WTG", "WGT AVG", "WGT_AVG"],
}

# ------------ Utilities ------------

def _std_col(name: str) -> str:
    """Uppercase and replace non-alnum with underscores: 'Unit Price' -> 'UNIT_PRICE'."""
    return re.sub(r"[^A-Z0-9]+", "_", str(name).upper()).strip("_")


def _match_col(std_cols: list[str], candidates: dict) -> dict:
    """Map our desired internal keys to the closest matching source column names."""
    found = {}
    std_lookup = {c: c for c in std_cols}
    for want, options in candidates.items():
        for opt in options:
            k = _std_col(opt)
            if k in std_lookup:
                found[want] = k
                break
    return found


def normalize_item_code(x: str) -> str:
    """
    Normalize pay item codes to a consistent form.
    - If there are 8 digits total, format as NNN-NNNNN (e.g., '30608033' -> '306-08033').
    - Replace long dashes with '-' and strip odd chars.
    """
    s = "" if x is None else str(x)
    digits = re.sub(r"\D", "", s)
    if len(digits) == 8:
        return f"{digits[:3]}-{digits[3:]}"

    # Fallback cleanup
    s = s.strip()
    for bad_dash in ("\u2014", "\u2013", "\u2012", "\u2011", "\u2212"):
        s = s.replace(bad_dash, "-")
    s = re.sub(r"[^\w\-]", "", s)
    return s




def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to our internal names and coerce basic types."""
    out = df.copy()
    out.columns = [_std_col(c) for c in out.columns]
    colmap = _match_col(list(out.columns), HEADER_MAP)

    rename = {}
    if "item_code" in colmap: rename[colmap["item_code"]] = "ITEM_CODE"
    if "desc"      in colmap: rename[colmap["desc"]]      = "DESCRIPTION"
    if "unit"      in colmap: rename[colmap["unit"]]      = "UNIT"
    if "qty"       in colmap: rename[colmap["qty"]]       = "QUANTITY"
    if "price"     in colmap: rename[colmap["price"]]     = "UNIT_PRICE"
    if "letting"   in colmap: rename[colmap["letting"]]   = "LETTING_DATE"  # alias BID_DATE -> LETTING_DATE
    if "county"    in colmap: rename[colmap["county"]]    = "COUNTY"
    if "district"  in colmap: rename[colmap["district"]]  = "DISTRICT"
    if "region"    in colmap: rename[colmap["region"]]    = "REGION"
    if "bidder"    in colmap: rename[colmap["bidder"]]    = "BIDDER"
    if "weight"    in colmap: rename[colmap["weight"]]    = "WEIGHT"

    if rename:
        out = out.rename(columns=rename)

    # Numeric coercions (quietly ignore failures; leave as-is if not numeric)
    for c in ["UNIT_PRICE", "QUANTITY", "WEIGHT", "REGION", "JOB_SIZE"]:
        if c in out.columns:
            try:
                out[c] = pd.to_numeric(out[c])
            except Exception:
                pass

    # String cleanups
    if "DISTRICT" in out.columns:
        out["DISTRICT"] = out["DISTRICT"].astype(str).str.upper().str.strip()
    if "COUNTY" in out.columns:
        out["COUNTY"] = out["COUNTY"].astype(str).str.upper().str.strip()
    if "ITEM_CODE" in out.columns:
        out["ITEM_CODE"] = out["ITEM_CODE"].map(normalize_item_code)

    return out

# ------------ Public loaders ------------

def load_bidtabs_files(folder: str | Path) -> pd.DataFrame:
    """
    Load and stack all CSV/XLS/XLSX files in a folder.
    - Reads all visible sheets from Excel workbooks.
    - Normalizes columns.
    """
    p = Path(folder)
    files = list(p.glob("*.csv")) + list(p.glob("*.xls")) + list(p.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError(f"No BidTabs files (.csv/.xls/.xlsx) found in {p}")

    dfs: list[pd.DataFrame] = []
    for f in files:
        ext = f.suffix.lower()
        if ext == ".csv":
            raw = pd.read_csv(f, dtype=str, encoding="utf-8", na_filter=False)
            if not raw.empty:
                dfs.append(_normalize_columns(raw))
        else:
            # Excel: read all sheets
            xl = pd.ExcelFile(f)
            for sh in xl.sheet_names:
                df = xl.parse(sh, dtype=str)
                if not df.empty:
                    dfs.append(_normalize_columns(df))

    if not dfs:
        raise ValueError(f"Parsed 0 rows from files in {p}")

    return pd.concat(dfs, ignore_index=True)


def ensure_region_column(bidtabs: pd.DataFrame, region_map: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Guarantee a numeric REGION column.
    - If REGION already exists and is numeric, return as-is.
    - If no region_map is supplied, pass through existing REGION or create an empty column.
    - Else, map from DISTRICT names using region_map (DISTRICT, REGION).
    """
    df = bidtabs.copy()

    # Already have a numeric REGION? Keep it.
    if "REGION" in df.columns and pd.api.types.is_numeric_dtype(df["REGION"]):
        return df

    # No map provided: ensure column exists and bail early.
    if region_map is None or getattr(region_map, "empty", False):
        if "REGION" not in df.columns:
            df["REGION"] = pd.NA
        return df

    # If we don't even have DISTRICT, create empty REGION
    if "DISTRICT" not in df.columns:
        df["REGION"] = pd.NA
        return df

    m = region_map.copy()
    # Normalize map columns
    cols = {c.upper().strip(): c for c in m.columns}
    dcol = cols.get("DISTRICT")
    rcol = cols.get("REGION")
    if not dcol or not rcol:
        raise KeyError("Region map must have DISTRICT and REGION columns")
    m = m.rename(columns={dcol: "DISTRICT", rcol: "REGION"})
    m["DISTRICT"] = m["DISTRICT"].astype(str).str.upper().str.strip()
    m["REGION"] = pd.to_numeric(m["REGION"], errors="coerce")

    # Merge and fill REGION
    df = df.merge(m, on="DISTRICT", how="left", suffixes=("", "_MAP"))
    if "REGION_MAP" in df.columns and "REGION" not in bidtabs.columns:
        df["REGION"] = df["REGION_MAP"]
        df = df.drop(columns=[c for c in ["REGION_MAP"] if c in df.columns])

    return df


def load_quantities(xlsx_path: str | Path) -> pd.DataFrame:
    """
    Load project quantities Excel.
    Required columns (any case/variant): ITEM_CODE (or PAY ITEM), DESCRIPTION, UNIT, QUANTITY
    """
    df = pd.read_excel(xlsx_path, engine="openpyxl", header=0)
    # Stringify headers and drop empty columns
    df.columns = [str(c) if c is not None else "" for c in df.columns]
    df = df.loc[:, [str(c).strip() != "" for c in df.columns]]

    cols = {str(c).upper().strip(): c for c in df.columns}

    def pick(*names: str) -> str:
        for n in names:
            if n in cols:
                return cols[n]
        raise KeyError(
            f"Missing one of {names} in quantities file: {xlsx_path}\n"
            f"Found columns: {list(df.columns)}"
        )

    df = df.rename(columns={
        # Accept PAY ITEM as the item-code header
        pick("ITEM_CODE", "ITEM", "ITEM NO", "PAY ITEM", "PAY_ITEM", "PAYITEM"): "ITEM_CODE",
        pick("DESCRIPTION", "ITEM_DESCRIPTION", "ITEM DESC"): "DESCRIPTION",
        pick("UNIT", "UOM"): "UNIT",
        pick("QUANTITY", "QTY"): "QUANTITY",
    })

    # Normalize item codes
    df["ITEM_CODE"] = df["ITEM_CODE"].map(normalize_item_code)
    return df


def load_region_map(source: Union[str, Path, pd.DataFrame]) -> pd.DataFrame:
    """
    Load region map (DISTRICT, REGION) from a dataframe or path and normalize columns.
    """
    if isinstance(source, pd.DataFrame):
        df = source.copy()
    else:
        p = Path(source)
        if not p.exists():
            raise FileNotFoundError(f"Region map not found at {p}")

        if p.suffix.lower() == ".xlsx":
            df = pd.read_excel(p, engine="openpyxl")
        else:
            df = pd.read_csv(p)

    if df.empty:
        return pd.DataFrame(columns=["DISTRICT", "REGION"])

    cols = {c.upper().strip(): c for c in df.columns}
    dcol = cols.get("DISTRICT")
    rcol = cols.get("REGION")
    if not dcol or not rcol:
        raise KeyError("Region map must have DISTRICT and REGION columns")

    out = df.rename(columns={dcol: "DISTRICT", rcol: "REGION"})
    out["DISTRICT"] = out["DISTRICT"].astype(str).str.upper().str.strip()
    out["REGION"] = pd.to_numeric(out["REGION"], errors="coerce")
    return out


def find_quantities_file(glob_pattern: str, base_dir: Union[str, Path]) -> Path:
    """
    Find a quantities file matching a glob pattern like:
      data_in\\???????_project_quantities.xlsx
    Searches relative to base_dir if the pattern isn't absolute.
    Returns the FIRST match (sorted by name) for determinism.
    """
    base = Path(base_dir)
    pat = Path(glob_pattern)
    search_root = pat if pat.is_absolute() else base / pat
    matches = sorted(search_root.parent.glob(search_root.name))
    if not matches:
        raise FileNotFoundError(f"No quantities file found matching {search_root}")
    return matches[0]


# Back-compat alias (in case other modules import the old name)
load_bidtabs_csvs = load_bidtabs_files

from __future__ import annotations

import csv
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from pdfminer.high_level import extract_text
from openpyxl import Workbook

from .cli import run
from .config import CLIConfig


ITEM_CODE_RE = re.compile(r"(?P<code>\b\d{3}[-–—]\d{5}\b)")


@dataclass(frozen=True)
class ParsedLine:
    code: str
    description: str
    unit: str
    quantity: float
    unit_price: float
    total: Optional[float] = None


def _to_float(s: str) -> Optional[float]:
    s = (s or "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def parse_bidtab_pdf(path: Path) -> pd.DataFrame:
    """Heuristic parser for official bid tab PDFs.

    Extracts rows with a pay item code like 123-45678 and attempts to parse unit, quantity,
    unit price, and total. This is a best-effort text parser tuned for typical Tab A tables.
    """
    text = extract_text(str(path)) or ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    parsed: List[ParsedLine] = []
    for ln in lines:
        m = ITEM_CODE_RE.search(ln)
        if not m:
            continue
        code = m.group("code").replace("\u2013", "-").replace("\u2014", "-")
        # Take a conservative split: try to find trailing numeric groups for qty, unit price, total
        # Example pattern: <desc> <UNIT> <QTY> <UNIT_PRICE> <TOTAL>
        tail_nums = re.findall(r"([A-Z/]+)?\s*([\d,]+(?:\.\d+)?)\s+([\d,]+(?:\.\d+)?)\s+([\d,]+(?:\.\d+)?)$", ln)
        unit = ""
        qty = None
        uprice = None
        total = None
        if tail_nums:
            unit, q, up, tot = tail_nums[-1]
            qty = _to_float(q)
            uprice = _to_float(up)
            total = _to_float(tot)
        else:
            # Fallback: grab last two numbers as unit price and total
            nums = re.findall(r"([\d,]+(?:\.\d+)?)", ln)
            if len(nums) >= 2:
                uprice = _to_float(nums[-2])
                total = _to_float(nums[-1])
        # Description heuristic: remove code and trailing numeric chunk
        desc_part = ln
        desc_part = desc_part.replace(code, "").strip()
        if total is not None:
            # remove last occurrence of total
            desc_part = re.sub(re.escape(f"{int(total):,}"), "", desc_part).strip()
        if uprice is not None:
            desc_part = re.sub(re.escape(f"{uprice:,.2f}").replace(",", ","), "", desc_part)
        # Collapse spaces
        desc_part = re.sub(r"\s{2,}", " ", desc_part).strip()
        if not unit and len(desc_part.split()) >= 1:
            # unit often is an all-caps token before the numbers; try last all-caps token
            tokens = desc_part.split()
            caps = [t for t in tokens if re.fullmatch(r"[A-Z/]+", t)]
            if caps:
                unit = caps[-1]
        # Description should exclude unit token if it's the last caps token
        if unit and desc_part.endswith(" " + unit):
            description = desc_part[: -len(unit)].strip()
        else:
            description = desc_part

        if uprice is None and total is not None and qty not in (None, 0):
            try:
                uprice = total / (qty or 1)
            except Exception:
                pass

        if uprice is None:
            continue
        parsed.append(ParsedLine(code=code, description=description, unit=unit, quantity=qty or 0.0, unit_price=uprice, total=total))

    if not parsed:
        return pd.DataFrame(columns=["ITEM_CODE", "DESCRIPTION", "UNIT", "QUANTITY", "ACTUAL_UNIT_PRICE", "ACTUAL_TOTAL"])

    df = pd.DataFrame([{
        "ITEM_CODE": p.code,
        "DESCRIPTION": p.description,
        "UNIT": p.unit,
        "QUANTITY": p.quantity,
        "ACTUAL_UNIT_PRICE": p.unit_price,
        "ACTUAL_TOTAL": p.total,
    } for p in parsed])
    # Keep only reasonable rows
    df = df.dropna(subset=["ACTUAL_UNIT_PRICE"]).copy()
    return df


def write_quantities_workbook(df: pd.DataFrame, destination: Path) -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = "PROJECT"
    ws.append(["ITEM_CODE", "DESCRIPTION", "UNIT", "QUANTITY"])
    for _, r in df.iterrows():
        ws.append([r.get("ITEM_CODE"), r.get("DESCRIPTION"), r.get("UNIT"), r.get("QUANTITY", 0)])
    destination.parent.mkdir(parents=True, exist_ok=True)
    wb.save(destination)
    wb.close()
    return destination


def evaluate_contract(pdf_path: Path, work_dir: Path) -> Tuple[pd.DataFrame, Path]:
    """Parse a PDF, generate quantities workbook, run CostEstimateGenerator, and compare to actuals.

    Returns (per_item_report_df, output_folder)
    """
    actual_df = parse_bidtab_pdf(pdf_path)
    if actual_df.empty:
        return pd.DataFrame(columns=["ITEM_CODE", "ACTUAL_UNIT_PRICE", "UNIT_PRICE_EST", "ABS_PCT_ERR", "ALTERNATE_USED"]).copy(), work_dir

    # Build temporary outputs for this contract
    out_dir = work_dir / pdf_path.stem
    out_dir.mkdir(parents=True, exist_ok=True)
    qty_xlsx = out_dir / "project_quantities.xlsx"
    write_quantities_workbook(actual_df[["ITEM_CODE", "DESCRIPTION", "UNIT", "QUANTITY"]], qty_xlsx)

    # Prepare config for run()
    estimate_xlsx = out_dir / "Estimate_Draft.xlsx"
    estimate_audit = out_dir / "Estimate_Audit.csv"
    payitems_audit = out_dir / "PayItems_Audit.xlsx"
    mapping_debug = out_dir / "payitem_mapping_debug.csv"
    cfg = CLIConfig(
        input_payitems=Path(".") / "payitems",  # not used directly by current pipeline
        estimate_audit_csv=estimate_audit,
        estimate_xlsx=estimate_xlsx,
        payitems_workbook=payitems_audit,
        mapping_debug_csv=mapping_debug,
        disable_ai=True,
        api_key_file=None,
        dry_run=False,
        log_level="INFO",
    )

    # Temporarily point environment override for quantities file
    import os
    prev_qty = os.environ.get("QUANTITIES_XLSX", "")
    prev_disable_filter = os.environ.get("DISABLE_CONTRACT_COST_FILTER")
    try:
        os.environ["QUANTITIES_XLSX"] = str(qty_xlsx)
        # Ensure benchmarking runs do not pre-filter contracts by expected cost
        os.environ["DISABLE_CONTRACT_COST_FILTER"] = "1"
        status = run(cfg)
    finally:
        if prev_qty:
            os.environ["QUANTITIES_XLSX"] = prev_qty
        else:
            os.environ.pop("QUANTITIES_XLSX", None)
        if prev_disable_filter is not None:
            os.environ["DISABLE_CONTRACT_COST_FILTER"] = prev_disable_filter
        else:
            os.environ.pop("DISABLE_CONTRACT_COST_FILTER", None)

    # Load produced audit and join with actuals
    if estimate_audit.exists():
        est = pd.read_csv(estimate_audit)
    else:
        est = pd.DataFrame(columns=["ITEM_CODE", "UNIT_PRICE_EST", "ALTERNATE_USED"])  # fallback
    est_cols = {c.upper(): c for c in est.columns}
    # Normalize column names we need
    if "ITEM_CODE" not in est.columns and "ITEM_CODE" in est_cols:
        est = est.rename(columns={est_cols["ITEM_CODE"]: "ITEM_CODE"})
    if "UNIT_PRICE_EST" not in est.columns and "UNIT_PRICE_EST" in est_cols:
        est = est.rename(columns={est_cols["UNIT_PRICE_EST"]: "UNIT_PRICE_EST"})
    if "ALTERNATE_USED" not in est.columns and "ALTERNATE_USED" in est_cols:
        est = est.rename(columns={est_cols["ALTERNATE_USED"]: "ALTERNATE_USED"})

    merged = actual_df.merge(est[[c for c in ["ITEM_CODE", "UNIT_PRICE_EST", "ALTERNATE_USED"] if c in est.columns]], on="ITEM_CODE", how="left")
    merged["ABS_PCT_ERR"] = (
        (merged["UNIT_PRICE_EST"].astype(float) - merged["ACTUAL_UNIT_PRICE"].astype(float)).abs()
        / merged["ACTUAL_UNIT_PRICE"].replace(0, math.nan).astype(float)
    )
    return merged, out_dir


def summarize_errors(rows: Iterable[dict]) -> dict:
    df = pd.DataFrame(list(rows))
    if df.empty:
        return {"count": 0, "rmse": math.nan, "mape": math.nan}
    dif = df["UNIT_PRICE_EST"].astype(float) - df["ACTUAL_UNIT_PRICE"].astype(float)
    rmse = float(math.sqrt((dif.pow(2)).mean())) if not dif.empty else math.nan
    mape = float((df["ABS_PCT_ERR"].dropna()).mean()) if "ABS_PCT_ERR" in df.columns else math.nan
    alt_mask = df.get("ALTERNATE_USED", pd.Series([False]*len(df))).fillna(False).astype(bool)
    return {
        "count": int(len(df)),
        "rmse": rmse,
        "mape": mape,
        "alt_count": int(alt_mask.sum()),
        "alt_mape": float(df.loc[alt_mask, "ABS_PCT_ERR"].mean()) if alt_mask.any() else math.nan,
    }

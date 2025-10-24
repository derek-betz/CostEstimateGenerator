"""Helpers for applying INDOT Design Memo 23-21 to HMA pay items."""

from __future__ import annotations

from dataclasses import dataclass
import csv
from pathlib import Path


@dataclass(frozen=True)
class CrosswalkRow:
    old_pay_item: str
    old_desc: str | None
    new_pay_item: str | None
    new_desc: str | None
    course: str | None
    esal_cat: str | None
    binder_class: str | None
    status: str  # ACTIVE or DELETED


def load_crosswalk(path: Path) -> dict[str, CrosswalkRow]:
    """Load the DM 23-21 crosswalk from ``path``."""

    x: dict[str, CrosswalkRow] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            x[r["old_pay_item"]] = CrosswalkRow(
                old_pay_item=r["old_pay_item"].strip(),
                old_desc=(r.get("old_desc", "").strip() or None),
                new_pay_item=(r["new_pay_item"].strip() or None),
                new_desc=(r.get("new_desc", "").strip() or None),
                course=(r["course"].strip() or None),
                esal_cat=(r["esal_cat"].strip() or None),
                binder_class=(r["binder_class"].strip() or None),
                status=r.get("status", "ACTIVE").strip().upper(),
            )
    return x


def remap_item(old_item: str, xwalk: dict[str, CrosswalkRow]) -> tuple[str | None, dict]:
    """Return (new_item_or_None, metadata)."""

    row = xwalk.get(old_item)
    if not row:
        return old_item, {"mapping_rule": None, "deleted": False}
    if row.status == "DELETED" or not row.new_pay_item:
        return None, {"mapping_rule": "DM 23-21", "deleted": True, "source_item": old_item}
    meta = {
        "mapping_rule": "DM 23-21",
        "source_item": old_item,
        "mapped_item": row.new_pay_item,
        "old_desc": row.old_desc,
        "new_desc": row.new_desc,
        "course": row.course,
        "esal_cat": row.esal_cat,
        "binder_class": row.binder_class,
    }
    return row.new_pay_item, meta


DM2321_ADDERS_PER_TON = {"Base": 2.00, "Intermediate": 2.50, "Surface": 3.00}


def maybe_apply_dm2321_adder(
    course: str | None,
    price: float,
    *,
    enabled: bool,
    sufficient_history: bool,
) -> tuple[float, bool]:
    """Apply the transitional DM 23-21 adder if appropriate."""

    if not enabled or sufficient_history:
        return price, False
    adder = DM2321_ADDERS_PER_TON.get((course or "").title())
    if not adder:
        return price, False
    return price + adder, True

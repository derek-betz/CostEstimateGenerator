"""
Static mappings between design memo replacements and obsolete pay items.

This module serves as a simple, hard-coded lookup so the pricing pipeline
can roll legacy bid data forward to newly issued pay item codes when
category lookups fail.  The mapping is intentionally compact and easy to
maintain by hand.  See the README for guidance on extending it.
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .bidtabs_io import normalize_item_code


@dataclass(frozen=True)
class DesignMemoMapping:
    """Container describing a replacement pay item and its obsolete sources."""

    memo_id: str
    effective_date: str
    obsolete_codes: List[str]


# Keep the initial mapping small but explicit so future updates are easy.
_DESIGN_MEMO_MAP: Dict[str, DesignMemoMapping] = {
    # Design Memo 25-10 (2025-04-16)
    normalize_item_code("401-11526"): DesignMemoMapping(
        memo_id="25-10",
        effective_date="2025-04-16",
        obsolete_codes=[
            normalize_item_code("401-10258"),
            normalize_item_code("401-10259"),
        ],
    ),
}

DEFAULT_MAPPING_PATH = Path("references/memos/mappings/design_memo_mappings.csv")


def get_obsolete_mapping(item_code: str) -> Optional[Dict[str, object]]:
    """
    Return design memo metadata for a replacement pay item code.

    Parameters
    ----------
    item_code:
        Target pay item code from the project quantities sheet.

    Returns
    -------
    dict | None
        Dictionary with keys ``memo_id``, ``effective_date``, and
        ``obsolete_codes`` when the given item has an associated design memo
        rollup, or ``None`` when no mapping is defined.

    Notes
    -----
    - Codes are normalized before lookup, so callers can pass values with
      or without dashes.
    - Future updates should extend ``_DESIGN_MEMO_MAP`` above and keep this
      function unchanged.  TODO: Consider loading mappings from an external
      spreadsheet once additional memos are published.
    """

    code = normalize_item_code(item_code)
    mapping = get_design_memo_mappings().get(code)
    if mapping is None:
        return None
    return {
        "memo_id": mapping.memo_id,
        "effective_date": mapping.effective_date,
        "obsolete_codes": list(mapping.obsolete_codes),
    }


def get_design_memo_mappings(path: Path | None = None) -> Dict[str, DesignMemoMapping]:
    """Return merged design memo mappings, including optional external data."""

    merged: Dict[str, DesignMemoMapping] = dict(_DESIGN_MEMO_MAP)
    external = load_additional_mappings(path)
    for code, mapping in external.items():
        merged.setdefault(code, mapping)
    return merged


def load_additional_mappings(path: Path | None = None) -> Dict[str, DesignMemoMapping]:
    """Load optional design memo mappings from CSV or JSON."""

    target = _resolve_mapping_path(path)
    if not target:
        return {}
    return _load_mapping_file(target)


def _resolve_mapping_path(path: Path | None) -> Optional[Path]:
    if path:
        candidate = Path(path)
    else:
        env_path = os.environ.get("DESIGN_MEMO_MAPPINGS_FILE")
        candidate = Path(env_path) if env_path else DEFAULT_MAPPING_PATH
    if candidate.exists():
        return candidate
    if candidate.suffix.lower() == ".csv":
        json_candidate = candidate.with_suffix(".json")
        if json_candidate.exists():
            return json_candidate
    return None


@lru_cache(maxsize=None)
def _load_mapping_file(path: Path) -> Dict[str, DesignMemoMapping]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        records = _load_csv(path)
    elif suffix == ".json":
        records = _load_json(path)
    else:
        return {}
    mappings: Dict[str, DesignMemoMapping] = {}
    for code, payload in records.items():
        mappings[code] = DesignMemoMapping(
            memo_id=payload["memo_id"],
            effective_date=payload["effective_date"],
            obsolete_codes=sorted({normalize_item_code(code) for code in payload["obsolete_codes"]}),
        )
    return mappings


def _load_csv(path: Path) -> Dict[str, dict]:
    grouped: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            replacement = normalize_item_code(row.get("replacement_code", ""))
            obsolete = normalize_item_code(row.get("obsolete_code", ""))
            memo_id = (row.get("memo_id") or "").strip()
            effective_date = (row.get("effective_date") or "").strip()
            if not replacement or not obsolete or not memo_id:
                continue
            bucket = grouped.setdefault(
                replacement,
                {"memo_id": memo_id, "effective_date": effective_date, "obsolete_codes": set()},
            )
            bucket["obsolete_codes"].add(obsolete)
            if effective_date:
                bucket["effective_date"] = effective_date
    for value in grouped.values():
        value["obsolete_codes"] = list(value["obsolete_codes"])
    return grouped


def _load_json(path: Path) -> Dict[str, dict]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, dict):
        records: Iterable[dict] = payload.get("mappings", [])  # type: ignore[assignment]
    else:
        records = payload
    grouped: Dict[str, dict] = {}
    for row in records:
        replacement = normalize_item_code(row.get("replacement_code", ""))
        memo_id = (row.get("memo_id") or "").strip()
        effective_date = (row.get("effective_date") or "").strip()
        if not replacement or not memo_id:
            continue
        obsolete_values = row.get("obsolete_code") or row.get("obsolete_codes") or []
        if isinstance(obsolete_values, str):
            obsolete_list = [obsolete_values]
        else:
            obsolete_list = list(obsolete_values)
        bucket = grouped.setdefault(
            replacement,
            {"memo_id": memo_id, "effective_date": effective_date, "obsolete_codes": set()},
        )
        for item in obsolete_list:
            normalized = normalize_item_code(str(item))
            if normalized:
                bucket["obsolete_codes"].add(normalized)
        if effective_date:
            bucket["effective_date"] = effective_date
    for value in grouped.values():
        value["obsolete_codes"] = list(value["obsolete_codes"])
    return grouped


__all__ = ["get_obsolete_mapping", "get_design_memo_mappings", "load_additional_mappings"]

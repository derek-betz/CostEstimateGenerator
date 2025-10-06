"""Load and cache reference datasets for alternate-seek enrichment."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Mapping

import pandas as pd

try:
    from PyPDF2 import PdfReader  # type: ignore
except ImportError as exc:  # pragma: no cover - runtime guard
    raise RuntimeError(
        "PyPDF2 must be installed to parse the Standard Specifications PDF"
    ) from exc

from .bidtabs_io import normalize_item_code

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data_sample"
CACHE_DIR = DATA_DIR / "cache"
PAYITEMS_XLSX = DATA_DIR / "CurrentEnglishPayItems" / "Current-English-Pay-Items9.3.xlsx"
UNIT_PRICE_XLSX = DATA_DIR / "UnitPriceSummaries" / "CY2024-Unit-Price-Summary.xlsx"
SPEC_PDF = DATA_DIR / "StandardSpecifications" / "2026-Standard-Specifications.pdf"

CACHE_DIR.mkdir(parents=True, exist_ok=True)

PAYITEM_CACHE = CACHE_DIR / "payitem_catalog.json"
UNIT_PRICE_CACHE = CACHE_DIR / "unit_price_summary.json"
SPEC_CACHE = CACHE_DIR / "spec_sections.json"

SECTION_RE = re.compile(r"^SECTION\s+(\d{3}(?:\.\d+)*)(?:\s+[-–]\s+(.+))?", re.IGNORECASE)


def _needs_refresh(source: Path, cache: Path) -> bool:
    if not cache.exists():
        return True
    try:
        return cache.stat().st_mtime < source.stat().st_mtime
    except FileNotFoundError:
        return True


@lru_cache()
def load_payitem_catalog() -> Dict[str, Dict[str, object]]:
    if not PAYITEMS_XLSX.exists():
        return {}
    if _needs_refresh(PAYITEMS_XLSX, PAYITEM_CACHE):
        df = pd.read_excel(PAYITEMS_XLSX, header=1)
        df = df.rename(
            columns={
                "SECTION": "section",
                "ITEM": "item_code",
                "DESCRITPTION": "description",
                "UNIT": "unit",
                "TYPE": "type",
                "COMMENTS": "comments",
                "MANDATORY SUPPLEMENTAL DESCRIPTION": "mandatory_supplemental",
            }
        )
        cleaned: Dict[str, Dict[str, object]] = {}
        for _, row in df.iterrows():
            raw_code = str(row.get("item_code", "")).strip()
            if not raw_code or raw_code.upper() == "ITEM":
                continue
            code = normalize_item_code(raw_code)
            cleaned[code] = {
                "section": str(row.get("section", "")).strip(),
                "description": str(row.get("description", "")).strip(),
                "unit": str(row.get("unit", "")).strip(),
                "type": str(row.get("type", "")).strip(),
                "comments": str(row.get("comments", "")).strip(),
                "mandatory_supplemental": str(row.get("mandatory_supplemental", "")).strip(),
            }
        PAYITEM_CACHE.write_text(json.dumps(cleaned, indent=2), encoding="utf-8")
    return json.loads(PAYITEM_CACHE.read_text(encoding="utf-8"))


@lru_cache()
def load_unit_price_summary() -> Dict[str, Dict[str, object]]:
    if not UNIT_PRICE_XLSX.exists():
        return {}
    if _needs_refresh(UNIT_PRICE_XLSX, UNIT_PRICE_CACHE):
        df = pd.read_excel(UNIT_PRICE_XLSX, sheet_name=0, header=6)
        df.columns = [
            "year",
            "section",
            "item_code",
            "description",
            "unit",
            "lowest",
            "highest",
            "weighted_average",
            "contracts",
            "total_value",
        ]
        cleaned: Dict[str, Dict[str, object]] = {}
        for _, row in df.iterrows():
            raw_code = str(row.get("item_code", "")).strip()
            if not raw_code or raw_code.upper().startswith("ITEM"):
                continue
            code = normalize_item_code(raw_code)
            try:
                weighted = float(row.get("weighted_average", 0) or 0)
            except Exception:
                weighted = 0.0
            cleaned[code] = {
                "year": int(row.get("year", 0) or 0),
                "section": str(row.get("section", "")).strip(),
                "description": str(row.get("description", "")).strip(),
                "unit": str(row.get("unit", "")).strip(),
                "weighted_average": weighted,
                "contracts": float(row.get("contracts", 0) or 0),
                "total_value": float(row.get("total_value", 0) or 0),
                "lowest": float(row.get("lowest", 0) or 0),
                "highest": float(row.get("highest", 0) or 0),
            }
        UNIT_PRICE_CACHE.write_text(json.dumps(cleaned, indent=2), encoding="utf-8")
    return json.loads(UNIT_PRICE_CACHE.read_text(encoding="utf-8"))


@lru_cache()
def load_spec_sections() -> Dict[str, Dict[str, object]]:
    if not SPEC_PDF.exists():
        return {}
    if _needs_refresh(SPEC_PDF, SPEC_CACHE):
        reader = PdfReader(str(SPEC_PDF))
        sections: Dict[str, Dict[str, object]] = {}
        current_section: Optional[Dict[str, object]] = None
        buffer: List[str] = []

        def _flush() -> None:
            nonlocal buffer, current_section, sections
            if current_section is None:
                buffer = []
                return
            text = "\n".join(buffer).strip()
            current_section["text"] = text
            sections[current_section["id"]] = {
                "id": current_section["id"],
                "title": current_section.get("title"),
                "page_start": current_section.get("page_start"),
                "page_end": current_section.get("page_end"),
                "text": text,
            }
            buffer = []
            current_section = None

        for page_index, page in enumerate(reader.pages, start=1):
            try:
                page_text = page.extract_text() or ""
            except Exception:
                page_text = ""
            lines = [ln.strip() for ln in page_text.splitlines()]
            for line in lines:
                match = SECTION_RE.match(line)
                if match:
                    _flush()
                    section_id = match.group(1)
                    title = match.group(2) or ""
                    current_section = {
                        "id": section_id,
                        "title": title.strip(),
                        "page_start": page_index,
                        "page_end": page_index,
                    }
                    buffer = []
                else:
                    if current_section is not None:
                        buffer.append(line)
            if current_section is not None:
                current_section["page_end"] = page_index
        _flush()
        SPEC_CACHE.write_text(json.dumps(sections, indent=2), encoding="utf-8")
    return json.loads(SPEC_CACHE.read_text(encoding="utf-8"))


def build_reference_bundle(item_code: str) -> Dict[str, object]:
    code = normalize_item_code(item_code)
    payitems = load_payitem_catalog()
    unit_prices = load_unit_price_summary()
    specs = load_spec_sections()

    payitem_info = payitems.get(code)
    section_id = payitem_info.get("section") if payitem_info else ""
    spec_text = None
    spec_meta: Optional[Dict[str, object]] = None
    if section_id:
        spec_meta = specs.get(str(section_id))
        if spec_meta is None and str(section_id).isdigit():
            # try zero padded three-digit lookup
            spec_meta = specs.get(str(section_id).zfill(3))
        if spec_meta is not None:
            spec_text = spec_meta.get("text")

    unit_price_info = unit_prices.get(code)

    related_items: List[Dict[str, object]] = []
    if section_id:
        for other_code, payload in unit_prices.items():
            if payload.get("section") == section_id and other_code != code:
                related_items.append(
                    {
                        "item_code": other_code,
                        "weighted_average": payload.get("weighted_average"),
                        "contracts": payload.get("contracts"),
                        "description": payload.get("description"),
                    }
                )
        related_items = sorted(
            related_items,
            key=lambda entry: (float(entry.get("contracts", 0) or 0), float(entry.get("weighted_average", 0) or 0)),
            reverse=True,
        )[:5]

    return {
        "item_code": code,
        "payitem": payitem_info,
        "unit_price": unit_price_info,
        "spec_section": spec_meta,
        "spec_text": spec_text,
        "related_items": related_items,
    }


__all__ = [
    "build_reference_bundle",
    "load_payitem_catalog",
    "load_unit_price_summary",
    "load_spec_sections",
]


def snapshot_reference_summary(max_examples: int = 5) -> Dict[str, object]:
    """Return a compact snapshot of reference datasets for LLM prompts."""
    payitems = load_payitem_catalog()
    unit_prices = load_unit_price_summary()
    specs = load_spec_sections()

    def _take_items(mapping: Mapping[str, object]) -> List[Dict[str, object]]:
        items: List[Dict[str, object]] = []
        for key, value in list(mapping.items())[:max_examples]:
            if isinstance(value, dict):
                payload = dict(value)
            else:
                payload = {"value": value}
            payload.setdefault("item_code", key)
            text_val = payload.get('text')
            if isinstance(text_val, str) and len(text_val) > 2000:
                payload['text'] = text_val[:2000] + ' …'
            items.append(payload)
        return items

    return {
        "payitem_count": len(payitems),
        "unit_price_count": len(unit_prices),
        "spec_section_count": len(specs),
        "sample_payitems": _take_items(payitems),
        "sample_unit_prices": _take_items(unit_prices),
        "sample_spec_sections": _take_items(specs),
    }
